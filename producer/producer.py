"""
============================================================
Kafka Transaction Producer
============================================================
Mengirim event transaksi ke Kafka topic 'transactions'.

Event Types:
  - Valid events      : transaksi normal yang lolos validasi
  - Invalid events    : amount negatif, terlalu besar, source tidak dikenal, timestamp rusak
  - Late events       : event dengan timestamp > 3 menit ke belakang (simulasi keterlambatan)
  - Duplicate events  : event yang dikirim dua kali (user_id + timestamp sama)

Usage:
  python producer.py
============================================================
"""

import json
import random
import signal
import sys
import time
import logging
from datetime import datetime, timezone, timedelta

from kafka import KafkaProducer
from kafka.errors import KafkaError

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
KAFKA_TOPIC = "transactions"

INTERVAL_MIN = 1   # detik
INTERVAL_MAX = 2   # detik

AMOUNT_MIN = 1
AMOUNT_MAX = 10_000_000

VALID_SOURCES = ["mobile", "web", "pos"]
INVALID_SOURCES = ["atm", "telegram-bot", "unknown", "api-v1"]

# User ID pool — beberapa user akan dipakai ulang untuk simulasi duplikat
USER_ID_POOL = [f"U{str(i).zfill(5)}" for i in range(1000, 1020)]


# ============================================================
# Global state untuk graceful shutdown
# ============================================================
running = True


def handle_shutdown(signum, frame):
    """Handle SIGINT (Ctrl+C) atau SIGTERM untuk shutdown bersih."""
    global running
    logger.warning("🛑 Menerima sinyal shutdown. Menghentikan producer...")
    running = False


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


# ============================================================
# Event Generators
# ============================================================

def now_utc() -> str:
    """Return timestamp ISO 8601 saat ini dalam UTC."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_valid_event() -> dict:
    """
    Buat event transaksi yang valid.
    - user_id  : random dari pool
    - amount   : random dalam range 1 - 10.000.000
    - timestamp: waktu sekarang
    - source   : salah satu dari mobile, web, pos
    """
    return {
        "user_id": random.choice(USER_ID_POOL),
        "amount": random.randint(AMOUNT_MIN, AMOUNT_MAX),
        "timestamp": now_utc(),
        "source": random.choice(VALID_SOURCES),
    }


def make_invalid_negative_amount() -> dict:
    """Invalid: amount bernilai negatif."""
    event = make_valid_event()
    event["amount"] = random.randint(-100_000, -1)
    logger.debug("  [INVALID] negative amount event dibuat")
    return event


def make_invalid_large_amount() -> dict:
    """Invalid: amount melebihi batas maksimum (10.000.000)."""
    event = make_valid_event()
    event["amount"] = random.randint(10_000_001, 999_999_999)
    logger.debug("  [INVALID] large amount event dibuat")
    return event


def make_invalid_timestamp() -> dict:
    """Invalid: timestamp dengan format yang salah / tidak bisa di-parse."""
    event = make_valid_event()
    bad_timestamps = [
        "14-12-2025 09:00:20",           # format salah
        "not-a-timestamp",               # bukan tanggal
        "2025/12/14 09:00:20",          # format slash
        "1734166820",                    # unix timestamp (bukan ISO)
        "",                             # kosong
    ]
    event["timestamp"] = random.choice(bad_timestamps)
    logger.debug("  [INVALID] invalid timestamp event dibuat")
    return event


def make_invalid_source() -> dict:
    """Invalid: source tidak dikenali (bukan mobile/web/pos)."""
    event = make_valid_event()
    event["source"] = random.choice(INVALID_SOURCES)
    logger.debug("  [INVALID] unknown source event dibuat")
    return event


def make_late_event(minutes_ago: int = None) -> dict:
    """
    Buat event dengan timestamp jauh di masa lalu.
    Simulasi: event yang terlambat datang ke sistem (late arrival).
    Late > 3 menit akan kena watermark dan masuk DLQ.
    """
    if minutes_ago is None:
        minutes_ago = random.randint(4, 10)   # selalu > 3 menit agar masuk DLQ

    past_time = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    event = make_valid_event()
    event["timestamp"] = past_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    logger.debug(f"  [LATE] event dibuat dengan timestamp {minutes_ago} menit lalu")
    return event


def make_duplicate_event(original: dict) -> dict:
    """
    Buat duplikat dari event yang sudah ada.
    user_id + timestamp sama → akan terdeteksi sebagai DUPLICATE_EVENT.
    """
    duplicate = dict(original)
    # Ubah amount sedikit agar kelihatan 'berbeda' tapi user_id+timestamp sama
    duplicate["amount"] = original["amount"] + random.randint(1, 100)
    logger.debug("  [DUPLICATE] duplicate event dibuat")
    return duplicate


# ============================================================
# Event Sequence Builder
# ============================================================

def build_event_sequence() -> list:
    """
    Buat sequence event dengan campuran valid, invalid, late, dan duplicate.

    Komposisi per siklus (10 event):
      - 5 valid
      - 1 invalid (amount negatif)
      - 1 invalid (amount terlalu besar)
      - 1 invalid (source tidak dikenal)
      - 1 late event
      - 1 duplicate (dari valid event sebelumnya)
    """
    sequence = []

    # 5 valid events
    valid_events = [make_valid_event() for _ in range(5)]
    sequence.extend(valid_events)

    # 3 invalid events
    sequence.append(make_invalid_negative_amount())
    sequence.append(make_invalid_large_amount())
    sequence.append(make_invalid_source())

    # 1 invalid timestamp
    sequence.append(make_invalid_timestamp())

    # 1 late event (5 menit lalu → masuk DLQ watermark)
    sequence.append(make_late_event(minutes_ago=random.randint(4, 8)))

    # 1 duplicate dari salah satu valid event
    original = random.choice(valid_events)
    sequence.append(make_duplicate_event(original))

    # Shuffle agar urutan acak (lebih realistis)
    random.shuffle(sequence)

    return sequence


# ============================================================
# Kafka Producer Setup
# ============================================================

def create_kafka_producer(bootstrap_servers: str, retries: int = 5) -> KafkaProducer:
    """
    Buat KafkaProducer dengan retry logic.

    Args:
        bootstrap_servers: Alamat Kafka broker
        retries: Jumlah percobaan ulang jika koneksi gagal

    Returns:
        KafkaProducer instance
    """
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                # Serialisasi value ke JSON bytes
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                # Key serialisasi ke bytes (pakai user_id sebagai key)
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                # Retry jika kirim gagal
                retries=3,
                # Timeout koneksi
                request_timeout_ms=30_000,
                # Batching untuk efisiensi
                batch_size=16_384,
                linger_ms=10,
                # Kompresi (opsional, bagus untuk produksi)
                compression_type="gzip",
            )
            logger.info(f"✅ Kafka producer terhubung ke {bootstrap_servers}")
            return producer

        except KafkaError as e:
            logger.warning(f"⚠️  Percobaan {attempt}/{retries} gagal: {e}")
            if attempt < retries:
                wait_time = 2 ** attempt   # Exponential backoff
                logger.info(f"   Mencoba lagi dalam {wait_time} detik...")
                time.sleep(wait_time)
            else:
                logger.error("❌ Gagal terhubung ke Kafka setelah semua percobaan.")
                raise


def send_event(producer: KafkaProducer, topic: str, event: dict) -> bool:
    """
    Kirim satu event ke Kafka topic.

    Args:
        producer: KafkaProducer instance
        topic: Nama Kafka topic
        event: Dictionary event yang akan dikirim

    Returns:
        True jika berhasil, False jika gagal
    """
    try:
        user_id = event.get("user_id", "unknown")
        amount = event.get("amount", 0)
        timestamp = event.get("timestamp", "N/A")
        source = event.get("source", "N/A")

        # Kirim event ke Kafka, gunakan user_id sebagai partition key
        future = producer.send(
            topic=topic,
            key=user_id,
            value=event,
        )

        # Block sampai pesan diterima broker (dengan timeout)
        record_metadata = future.get(timeout=10)

        logger.info(
            f"📤 SENT  | user={user_id:10s} | amount={str(amount):>12s} "
            f"| source={source:10s} | ts={timestamp} "
            f"| partition={record_metadata.partition} offset={record_metadata.offset}"
        )
        return True

    except KafkaError as e:
        logger.error(f"❌ FAILED | Gagal kirim event: {e} | event={event}")
        return False
    except Exception as e:
        logger.error(f"❌ ERROR  | Unexpected error: {e} | event={event}")
        return False


# ============================================================
# Main Producer Loop
# ============================================================

def run_producer():
    """
    Main loop producer:
    1. Buat koneksi ke Kafka
    2. Generate event sequence
    3. Kirim event satu per satu dengan interval 1-2 detik
    4. Shutdown bersih saat menerima sinyal
    """
    logger.info("=" * 60)
    logger.info("🚀 Kafka Transaction Producer Dimulai")
    logger.info(f"   Broker : {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"   Topic  : {KAFKA_TOPIC}")
    logger.info(f"   Rate   : {INTERVAL_MIN}-{INTERVAL_MAX} detik/event")
    logger.info("=" * 60)

    # Koneksi ke Kafka (dengan retry)
    try:
        producer = create_kafka_producer(KAFKA_BOOTSTRAP_SERVERS)
    except Exception as e:
        logger.error(f"❌ Tidak bisa membuat producer: {e}")
        sys.exit(1)

    total_sent = 0
    total_failed = 0
    cycle = 0

    try:
        while running:
            cycle += 1
            logger.info(f"\n{'─'*60}")
            logger.info(f"📦 Siklus #{cycle}: Membuat batch event...")

            # Generate sequence event untuk siklus ini
            events = build_event_sequence()
            logger.info(f"   Total event dalam siklus: {len(events)}")

            for event in events:
                if not running:
                    break

                success = send_event(producer, KAFKA_TOPIC, event)

                if success:
                    total_sent += 1
                else:
                    total_failed += 1

                # Tunggu interval random sebelum kirim event berikutnya
                sleep_time = random.uniform(INTERVAL_MIN, INTERVAL_MAX)
                time.sleep(sleep_time)

    finally:
        # Flush semua pesan yang masih di buffer sebelum tutup
        logger.info("\n📊 Menutup producer...")
        producer.flush(timeout=30)
        producer.close()

        logger.info("=" * 60)
        logger.info(f"✅ Producer selesai.")
        logger.info(f"   Total berhasil dikirim : {total_sent}")
        logger.info(f"   Total gagal           : {total_failed}")
        logger.info("=" * 60)


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":
    run_producer()
