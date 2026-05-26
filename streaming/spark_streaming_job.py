"""
============================================================
PySpark Structured Streaming - Transaction Pipeline
============================================================
Job ini membaca event transaksi dari Kafka, memvalidasi,
lalu me-routing ke topic valid atau DLQ (Dead Letter Queue).

Flow:
  Kafka (transactions)
    → Parse JSON
    → Validasi (mandatory fields, type, amount, source)
    → Detect duplicate (user_id + timestamp)
    → Detect late event (watermark 3 menit)
    → Route: valid → transactions_valid
             invalid → transactions_dlq
    → Tumbling Window (1 menit) → Console Output

Jalankan dengan:
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    streaming/spark_streaming_job.py
============================================================
"""

import sys
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, TimestampType, DoubleType
)
from pyspark.sql.window import Window

# ============================================================
# Logging Setup
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ============================================================
# Konfigurasi
# ============================================================
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC_INPUT       = "transactions"
KAFKA_TOPIC_VALID       = "transactions_valid"
KAFKA_TOPIC_DLQ         = "transactions_dlq"

CHECKPOINT_DIR          = "./checkpoints"
WATERMARK_DELAY         = "3 minutes"
WINDOW_DURATION         = "1 minute"

AMOUNT_MIN              = 1
AMOUNT_MAX              = 10_000_000
VALID_SOURCES           = ["mobile", "web", "pos"]


# ============================================================
# Schema Definisi (Explicit Schema — Best Practice)
# ============================================================
# Schema ini digunakan untuk parse JSON dari Kafka
# Jika field tidak ada → null (akan terdeteksi oleh validasi)
TRANSACTION_SCHEMA = StructType([
    StructField("user_id",   StringType(),  nullable=True),
    StructField("amount",    DoubleType(),  nullable=True),   # Double agar bisa detect "abc"
    StructField("timestamp", StringType(),  nullable=True),   # String dulu, lalu cast ke timestamp
    StructField("source",    StringType(),  nullable=True),
])


# ============================================================
# SparkSession Setup
# ============================================================

def create_spark_session(app_name: str = "TransactionStreamingJob") -> SparkSession:
    """
    Buat SparkSession dengan konfigurasi untuk streaming + Kafka.

    Konfigurasi penting:
    - spark.sql.streaming.statefulOperator.checkCorrectness.enabled=false
      → Matikan warning strict untuk demo/assignment
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        # Kurangi log Spark yang noise (ubah ke INFO untuk debugging)
        .config("spark.ui.showConsoleProgress", "false")
        # Checkpoint location untuk state (deduplication + watermark)
        .config("spark.sql.shuffle.partitions", "3")
        # Supaya foreachBatch tidak error saat tulis ke Kafka
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ============================================================
# Validation Logic
# ============================================================

def build_validation_columns(df):
    """
    Tambahkan kolom is_valid dan error_reason ke DataFrame.

    Validation rules (urutan prioritas):
    1. MISSING_USER_ID       : user_id null atau kosong
    2. MISSING_AMOUNT        : amount null
    3. MISSING_TIMESTAMP     : timestamp null atau kosong
    4. INVALID_TIMESTAMP_FMT : timestamp tidak bisa di-parse ke datetime
    5. INVALID_AMOUNT_RANGE  : amount < 1 atau amount > 10.000.000
    6. INVALID_SOURCE        : source bukan mobile/web/pos
    (Duplicate dan late event ditangani secara terpisah)

    Returns:
        DataFrame dengan kolom tambahan:
        - is_valid     (boolean)
        - error_reason (string, null jika valid)
    """

    # --- Parse timestamp string → event_time ---
    # to_timestamp() return null jika format tidak cocok
    df = df.withColumn(
        "event_time",
        F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )

    # --- Validation Rules (menggunakan CASE WHEN berantai) ---
    # Urutan penting: rule pertama yang match yang dipakai
    error_reason_expr = (
        F.when(
            F.col("user_id").isNull() | (F.trim(F.col("user_id")) == ""),
            F.lit("MISSING_USER_ID")
        )
        .when(
            F.col("amount").isNull(),
            F.lit("MISSING_AMOUNT")
        )
        .when(
            F.col("timestamp").isNull() | (F.trim(F.col("timestamp")) == ""),
            F.lit("MISSING_TIMESTAMP")
        )
        .when(
            F.col("event_time").isNull(),
            F.lit("INVALID_TIMESTAMP_FORMAT")
        )
        .when(
            (F.col("amount") < AMOUNT_MIN) | (F.col("amount") > AMOUNT_MAX),
            F.lit("INVALID_AMOUNT_RANGE")
        )
        .when(
            ~F.col("source").isin(VALID_SOURCES),
            F.lit("INVALID_SOURCE")
        )
        .otherwise(F.lit(None).cast(StringType()))   # null = valid sejauh ini
    )

    df = df.withColumn("error_reason", error_reason_expr)
    df = df.withColumn("is_valid", F.col("error_reason").isNull())

    return df


def detect_late_events(df):
    """
    Tandai event yang terlambat lebih dari 3 menit sebagai invalid.

    Late event = event_time < (waktu_proses - 3 menit)
    Event ini masuk DLQ dengan error LATE_EVENT.

    Catatan: withWatermark() di Spark menangani late data secara otomatis
    untuk stateful operations. Di sini kita eksplisit tandai untuk DLQ.
    """
    late_threshold = F.current_timestamp() - F.expr("INTERVAL 3 MINUTES")

    df = df.withColumn(
        "is_late",
        F.when(
            F.col("event_time").isNotNull() & (F.col("event_time") < late_threshold),
            F.lit(True)
        ).otherwise(F.lit(False))
    )

    # Update is_valid dan error_reason untuk late events
    df = df.withColumn(
        "error_reason",
        F.when(
            F.col("is_late") & F.col("is_valid"),   # hanya jika belum ada error lain
            F.lit("LATE_EVENT")
        ).otherwise(F.col("error_reason"))
    )
    df = df.withColumn(
        "is_valid",
        F.col("is_valid") & ~F.col("is_late")
    )

    return df


def detect_duplicates_in_batch(batch_df):
    """
    Deteksi duplikat dalam satu micro-batch menggunakan Window function.

    Duplikat = user_id + timestamp sama.
    Event pertama dianggap valid, sisanya DUPLICATE_EVENT.

    Kenapa per-batch:
    - Spark Streaming stateful deduplication membutuhkan withWatermark
    - Untuk assignment ini, intra-batch deduplication sudah cukup demonstratif
    - Cross-batch duplicate bisa ditambahkan dengan foreachBatch + external state
    """
    # Window: partisi berdasarkan user_id + timestamp, urut berdasarkan processing time
    window_spec = (
        Window
        .partitionBy("user_id", "timestamp")
        .orderBy(F.monotonically_increasing_id())
    )

    # Beri nomor urut dalam setiap group (user_id + timestamp)
    batch_df = batch_df.withColumn("_row_num", F.row_number().over(window_spec))

    # Row dengan nomor > 1 = duplikat
    batch_df = batch_df.withColumn(
        "is_duplicate",
        F.col("_row_num") > 1
    )

    # Update is_valid dan error_reason
    batch_df = batch_df.withColumn(
        "error_reason",
        F.when(
            F.col("is_duplicate") & F.col("is_valid"),
            F.lit("DUPLICATE_EVENT")
        ).otherwise(F.col("error_reason"))
    )
    batch_df = batch_df.withColumn(
        "is_valid",
        F.col("is_valid") & ~F.col("is_duplicate")
    )

    # Hapus kolom helper
    batch_df = batch_df.drop("_row_num", "is_duplicate", "is_late")

    return batch_df


# ============================================================
# Output Formatters
# ============================================================

def to_kafka_json(df, topic_name: str):
    """
    Ubah DataFrame menjadi format yang siap dikirim ke Kafka.
    Kafka membutuhkan kolom 'value' berisi bytes/string.
    """
    return (
        df.select(
            # Konversi seluruh row ke JSON string
            F.to_json(F.struct(*df.columns)).alias("value")
        )
    )


def write_to_kafka(df, topic: str, checkpoint_suffix: str):
    """
    Write DataFrame ke Kafka topic sebagai streaming sink.
    """
    return (
        df.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", topic)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/{checkpoint_suffix}")
        .outputMode("append")
        .start()
    )


# ============================================================
# Running Total (State)
# ============================================================
# Sederhana: dictionary untuk tracking total per window
# Untuk produksi, gunakan external store (Redis, DB)
window_running_totals: dict = {}


def print_window_summary(window_df):
    """
    Print summary tumbling window ke console.
    Format: timestamp | window | total_transactions | total_amount | running_total
    """
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Collect hasil aggregasi (dalam foreachBatch, ini aman untuk batch kecil)
    rows = window_df.collect()

    if not rows:
        return

    print("\n" + "═" * 72)
    print(f"  📊 WINDOW AGGREGATION  |  Processing Time: {now_str}")
    print("═" * 72)
    print(f"  {'Window Start':<20} {'Window End':<20} {'Trx Count':>10} {'Total Amount':>15}")
    print("─" * 72)

    total_this_batch = 0

    for row in rows:
        window_key = str(row["window_start"])

        # Update running total per window
        prev_total = window_running_totals.get(window_key, 0)
        window_running_totals[window_key] = prev_total + row["total_amount"]
        running_total = window_running_totals[window_key]

        total_this_batch += row["trx_count"]

        win_start = str(row["window_start"])[:19]
        win_end   = str(row["window_end"])[:19]

        print(
            f"  {win_start:<20} {win_end:<20} "
            f"{row['trx_count']:>10,} {row['total_amount']:>15,.0f}"
        )

    print("─" * 72)
    print(f"  ⏱  Timestamp      : {now_str}")
    print(f"  📈 Running Total  : Rp {sum(window_running_totals.values()):>20,.0f}")
    print("═" * 72 + "\n")


# ============================================================
# Batch Processor (foreachBatch)
# ============================================================

def process_batch(batch_df, batch_id: int):
    """
    Proses setiap micro-batch dari Kafka stream.

    Steps:
    1. Detect in-batch duplicates
    2. Split valid vs invalid
    3. Write valid   → Kafka transactions_valid
    4. Write invalid → Kafka transactions_dlq
    5. Print console summary
    6. Compute tumbling window aggregation
    """
    if batch_df.isEmpty():
        logger.info(f"⏩ Batch #{batch_id}: kosong, skip.")
        return

    batch_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_rows = batch_df.count()
    logger.info(f"\n{'='*60}")
    logger.info(f"🔄 BATCH #{batch_id}  |  {batch_start}  |  {total_rows} records")

    # --- Step 1: Detect in-batch duplicates ---
    batch_df = detect_duplicates_in_batch(batch_df)

    # Cache batch untuk reuse (avoid recomputation)
    batch_df.cache()

    # --- Step 2: Split valid vs invalid ---
    valid_df   = batch_df.filter(F.col("is_valid") == True)
    invalid_df = batch_df.filter(F.col("is_valid") == False)

    valid_count   = valid_df.count()
    invalid_count = invalid_df.count()

    logger.info(f"   ✅ Valid   : {valid_count} records → {KAFKA_TOPIC_VALID}")
    logger.info(f"   ❌ Invalid : {invalid_count} records → {KAFKA_TOPIC_DLQ}")

    # --- Step 3: Write valid → Kafka transactions_valid ---
    if valid_count > 0:
        (
            to_kafka_json(valid_df, KAFKA_TOPIC_VALID)
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("topic", KAFKA_TOPIC_VALID)
            .save()
        )

    # --- Step 4: Write invalid → Kafka transactions_dlq ---
    if invalid_count > 0:
        (
            to_kafka_json(invalid_df, KAFKA_TOPIC_DLQ)
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("topic", KAFKA_TOPIC_DLQ)
            .save()
        )

    # --- Step 5: Console summary per-record ---
    print_batch_summary(batch_df, batch_id)

    # --- Step 6: Tumbling Window Aggregation (hanya dari valid events) ---
    if valid_count > 0:
        window_df = compute_tumbling_window(valid_df)
        print_window_summary(window_df)

    batch_df.unpersist()


def print_batch_summary(batch_df, batch_id: int):
    """
    Print detail setiap record dalam batch ke console.
    """
    print(f"\n{'─'*80}")
    print(f"  BATCH #{batch_id} DETAIL")
    print(f"  {'user_id':<12} {'amount':>12} {'source':<10} {'is_valid':<10} {'error_reason':<25}")
    print(f"{'─'*80}")

    rows = batch_df.select(
        "user_id", "amount", "source", "is_valid", "error_reason"
    ).collect()

    for row in rows:
        status   = "✅" if row["is_valid"] else "❌"
        user_id  = str(row["user_id"] or "NULL")[:12]
        amount   = f"{row['amount']:>12,.0f}" if row["amount"] is not None else "        NULL"
        source   = str(row["source"] or "NULL")[:10]
        is_valid = str(row["is_valid"])
        reason   = str(row["error_reason"] or "-")[:25]

        print(f"  {status} {user_id:<12} {amount} {source:<10} {is_valid:<10} {reason:<25}")

    print(f"{'─'*80}")


def compute_tumbling_window(valid_df):
    """
    Hitung tumbling window aggregation dari valid events.

    Window:
    - Durasi    : 1 menit
    - Basis     : event_time (bukan processing time)
    - Watermark : 3 menit (sudah diterapkan sebelum masuk sini)

    Output:
    - window_start
    - window_end
    - trx_count     : jumlah transaksi dalam window
    - total_amount  : total nilai transaksi dalam window
    """
    window_df = (
        valid_df
        .filter(F.col("event_time").isNotNull())
        .groupBy(
            F.window(
                F.col("event_time"),
                windowDuration=WINDOW_DURATION
            ).alias("win")
        )
        .agg(
            F.count("*").alias("trx_count"),
            F.sum("amount").alias("total_amount"),
        )
        .select(
            F.col("win.start").alias("window_start"),
            F.col("win.end").alias("window_end"),
            F.col("trx_count"),
            F.col("total_amount"),
        )
        .orderBy("window_start")
    )

    return window_df


# ============================================================
# Main Streaming Job
# ============================================================

def run_streaming_job():
    """
    Entry point PySpark Structured Streaming job.

    Architecture:
    1. Buat SparkSession
    2. Baca dari Kafka topic 'transactions'
    3. Parse JSON payload
    4. Validasi field + tipe + range
    5. Tandai late events
    6. Proses per-batch dengan foreachBatch
    """
    logger.info("=" * 60)
    logger.info("🚀 Spark Structured Streaming Job Dimulai")
    logger.info(f"   Input  : kafka://{KAFKA_BOOTSTRAP_SERVERS}/{KAFKA_TOPIC_INPUT}")
    logger.info(f"   Valid  : kafka://{KAFKA_BOOTSTRAP_SERVERS}/{KAFKA_TOPIC_VALID}")
    logger.info(f"   DLQ    : kafka://{KAFKA_BOOTSTRAP_SERVERS}/{KAFKA_TOPIC_DLQ}")
    logger.info(f"   Watermark : {WATERMARK_DELAY}")
    logger.info(f"   Window    : {WINDOW_DURATION} tumbling")
    logger.info("=" * 60)

    # --- Step 1: SparkSession ---
    spark = create_spark_session()

    # --- Step 2: Baca dari Kafka ---
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_INPUT)
        # Mulai dari earliest untuk tidak kehilangan data di awal
        .option("startingOffsets", "earliest")
        # Batas pesan per micro-batch (hindari batch terlalu besar)
        .option("maxOffsetsPerTrigger", 200)
        .option("failOnDataLoss", "false")
        .load()
    )

    # --- Step 3: Extract value dari Kafka (bytes → string) ---
    # Kafka message format: key (bytes), value (bytes), topic, partition, offset, timestamp
    string_stream = raw_stream.select(
        F.col("value").cast("string").alias("raw_json"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("offset").alias("kafka_offset"),
        F.col("partition").alias("kafka_partition"),
    )

    # --- Step 4: Parse JSON dengan schema eksplisit ---
    parsed_stream = string_stream.select(
        F.from_json(F.col("raw_json"), TRANSACTION_SCHEMA).alias("data"),
        F.col("kafka_timestamp"),
        F.col("kafka_offset"),
        F.col("kafka_partition"),
        F.col("raw_json"),
    ).select(
        # Flatten nested struct 'data' menjadi kolom biasa
        F.col("data.user_id"),
        F.col("data.amount"),
        F.col("data.timestamp"),
        F.col("data.source"),
        F.col("kafka_timestamp"),
        F.col("kafka_offset"),
        F.col("kafka_partition"),
        F.col("raw_json"),
    )

    # --- Step 5: Tambahkan kolom validasi ---
    validated_stream = build_validation_columns(parsed_stream)

    # --- Step 6: Tandai late events ---
    # Catatan: withWatermark harus dipanggil SEBELUM groupBy untuk window aggregation
    # Di sini kita apply pada stream sebelum foreachBatch
    late_marked_stream = detect_late_events(validated_stream)

    # --- Step 7: Apply Watermark ---
    # withWatermark memberitahu Spark berapa lama menunggu late data
    # Event dengan event_time > 3 menit lebih tua dari max event_time akan dibuang
    # dari state store (untuk deduplication dan windowed aggregation)
    watermarked_stream = late_marked_stream.withWatermark(
        "event_time",
        WATERMARK_DELAY
    )

    # --- Step 8: Proses per batch dengan foreachBatch ---
    # foreachBatch memberi akses ke static DataFrame API (lebih fleksibel)
    query = (
        watermarked_stream.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/main")
        .outputMode("append")
        .trigger(processingTime="10 seconds")   # Proses setiap 10 detik
        .start()
    )

    logger.info("✅ Streaming job berjalan. Tekan Ctrl+C untuk berhenti.\n")

    try:
        # Tunggu hingga query selesai atau error
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("\n🛑 Menghentikan streaming job...")
        query.stop()
        spark.stop()
        logger.info("✅ Streaming job berhenti dengan bersih.")
    except Exception as e:
        logger.error(f"❌ Error pada streaming job: {e}")
        query.stop()
        spark.stop()
        sys.exit(1)


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":
    run_streaming_job()
