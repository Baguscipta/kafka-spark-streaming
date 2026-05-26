# ============================================================
# Makefile - Kafka Spark Streaming Project
# Usage: make <target>
# ============================================================

.PHONY: help up down topic producer streaming clean logs status

# Default target
help:
	@echo ""
	@echo "╔══════════════════════════════════════════════════════╗"
	@echo "║     Kafka + PySpark Streaming Pipeline               ║"
	@echo "╚══════════════════════════════════════════════════════╝"
	@echo ""
	@echo "  make up          → Jalankan Docker (Kafka + ZooKeeper + UI)"
	@echo "  make down        → Matikan Docker containers"
	@echo "  make topic       → Buat Kafka topics"
	@echo "  make producer    → Jalankan Kafka producer"
	@echo "  make streaming   → Jalankan PySpark streaming job"
	@echo "  make logs        → Lihat log Kafka broker"
	@echo "  make status      → Cek status containers"
	@echo "  make clean       → Hapus containers, volume, checkpoint"
	@echo "  make install     → Install semua Python dependencies"
	@echo ""

# ── Docker ────────────────────────────────────────────────

up:
	@echo "🚀 Menjalankan Docker containers..."
	docker-compose up -d
	@echo "⏳ Menunggu Kafka siap..."
	@sleep 15
	@echo "✅ Containers berjalan!"
	@echo "   Kafka UI: http://localhost:8080"

down:
	@echo "🛑 Menghentikan containers..."
	docker-compose down

logs:
	docker-compose logs -f kafka

status:
	docker-compose ps

# ── Kafka Topics ──────────────────────────────────────────

topic:
	@echo "📌 Membuat Kafka topics..."
	docker exec kafka kafka-topics --create \
		--bootstrap-server localhost:9092 \
		--replication-factor 1 \
		--partitions 3 \
		--topic transactions \
		--if-not-exists
	docker exec kafka kafka-topics --create \
		--bootstrap-server localhost:9092 \
		--replication-factor 1 \
		--partitions 3 \
		--topic transactions_valid \
		--if-not-exists
	docker exec kafka kafka-topics --create \
		--bootstrap-server localhost:9092 \
		--replication-factor 1 \
		--partitions 3 \
		--topic transactions_dlq \
		--if-not-exists
	@echo "✅ Topics berhasil dibuat:"
	docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

list-topics:
	docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

describe-topics:
	docker exec kafka kafka-topics --describe --bootstrap-server localhost:9092

# ── Python ────────────────────────────────────────────────

install:
	@echo "📦 Install producer dependencies..."
	pip install -r producer/requirements.txt
	@echo "📦 Install streaming dependencies..."
	pip install -r streaming/requirements.txt
	@echo "✅ Semua dependencies terinstall!"

producer:
	@echo "🚀 Menjalankan Kafka Producer..."
	cd producer && python producer.py

streaming:
	@echo "🚀 Menjalankan PySpark Streaming Job..."
	spark-submit \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
		--conf spark.ui.showConsoleProgress=false \
		streaming/spark_streaming_job.py

# ── Monitoring (consume dari console) ─────────────────────

consume-input:
	@echo "👀 Melihat topic: transactions"
	docker exec kafka kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic transactions \
		--from-beginning \
		--max-messages 20

consume-valid:
	@echo "👀 Melihat topic: transactions_valid"
	docker exec kafka kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic transactions_valid \
		--from-beginning \
		--max-messages 20

consume-dlq:
	@echo "👀 Melihat topic: transactions_dlq"
	docker exec kafka kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic transactions_dlq \
		--from-beginning \
		--max-messages 20

# ── Cleanup ───────────────────────────────────────────────

clean:
	@echo "🧹 Membersihkan resources..."
	docker-compose down -v
	rm -rf checkpoints/
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	@echo "✅ Cleanup selesai!"
