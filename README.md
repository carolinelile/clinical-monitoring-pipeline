# Real-Time Clinical Vital-Signs Monitoring Pipeline

A portfolio-grade data engineering project: a real-time pipeline that ingests
patient vital signs from a live **HAPI FHIR** REST API (populated with synthetic
Synthea data), streams them through **Kafka**, processes them with **Spark
Structured Streaming** (flagging abnormal readings and windowed aggregation),
and writes partitioned **Parquet** to S3-compatible storage. Fully containerized.

This is the **analytics/storage backbone** that sits *behind* bedside monitors —
not a replacement for life-critical bedside alarms, but the layer used for
cross-patient aggregation, persistence, trend/ML-based deterioration detection,
and multi-system (EHR/device) integration.

## Architecture

```
Synthea (FHIR R4 bundles)
   └── loaded via REST → HAPI FHIR server (Docker, :8080)   ← live FHIR REST API
                              │  producer polls GET /fhir/Observation?category=vital-signs
                              ▼
                         Kafka topic "clinical-vitals"
                              ▼
              Spark Structured Streaming (parse + flag abnormal + window)
                              ▼
         MinIO/S3 → partitioned Parquet  +  abnormal_events (alerts)
```

| Stage | Tech | Role | Cloud-portable to |
|-------|------|------|-------------------|
| Source | HAPI FHIR (R4) | Standards-based health data API; vitals as `Observation` resources (LOINC-coded) | AWS HealthLake |
| Transport | Apache Kafka (KRaft mode) | Durable streaming buffer; decouples producer from consumer; replay/backpressure | AWS MSK / Kinesis |
| Processing | Spark Structured Streaming | Micro-batch stream processing; flags vitals, windowed aggregation | EMR / Glue / Databricks |
| Storage | MinIO | S3-compatible object storage; Parquet partitioned by date | Amazon S3 |

## Phase 1 — Local build (cloud-free) · 2 weeks, Jun 8–21 2026

Everything runs locally via `docker-compose` and Python. Free, fast to iterate,
and skill-equivalent to the cloud version (MinIO speaks the S3 API; Kafka/Spark
are the real open-source engines). Work in this order:

| Day | Deliverable |
|-----|-------------|
| **Day 1** | Repo + `docker-compose` (HAPI FHIR + Kafka + MinIO); all healthy; create `clinical-data` bucket |
| **Day 2** | Generate Synthea data; `load_to_fhir.py` POSTs bundles as transactions; verify via API |
| **Day 3** | `fhir_client.py`: query `Observation?category=vital-signs`, follow `Bundle.link` pagination, parse to `{patient_id, vital_type, value, unit, timestamp}` |
| **Day 4** | `producer.py`: poll FHIR API → publish JSON events to Kafka |
| **Day 5** | `streaming_job.py` skeleton: Spark readStream from Kafka, explicit schema, write to console |
| **Day 6** | Abnormal flagging: normal ranges in config; add `is_abnormal` + `severity` |
| **Day 7** | Parquet sink to MinIO, partitioned by date, with checkpointing |
| **Day 8** | Windowed per-patient aggregation (5-min window, watermark) + `abnormal_events` table |
| **Day 9** | Robustness (schema enforcement, dead-letter path, logging) + pytest + data-quality checks |
| **Day 10** | README (problem, Mermaid architecture, run steps, screenshots, design decisions); demo GIF; pin repo |

Vital-sign normal ranges: HR 60–100 bpm, SpO₂ ≥ 92%, temp 36.1–37.2 °C,
respiratory rate 12–20. Severity: normal / warning / critical.

### Quick start (Phase 1)

```bash
# 1. Start Docker Desktop, then:
docker compose up -d

# 2. Verify all services healthy
docker compose ps

# 3. Endpoints
#    HAPI FHIR     -> http://localhost:8080    (FHIR R4 API + UI)
#    MinIO API     -> http://localhost:9000
#    MinIO console -> http://localhost:9001    (minioadmin / minioadmin)
```

## Phase 2 — AWS deployment (cloud, production-shaped)

Phase 1 proves the pipeline logic locally. Phase 2 ports it to AWS to mirror a
real production deployment and to demonstrate the **cloud + infrastructure-as-code**
skills that production data-engineering roles require. The migration is small by
design — we chose S3-compatible and open-source tools precisely so the move is
mostly config, not rewrites.

**Goal:** the same FHIR → stream → Spark → object-store flow, running on managed
AWS services, provisioned with Terraform, with IAM/VPC, cost controls, and basic
monitoring.

### Migration path (cheapest / highest-value first)

| Step | Local component | AWS target | Notes |
|------|-----------------|------------|-------|
| 2.1 | MinIO | **Amazon S3** | Smallest change (swap endpoint + credentials); near-zero cost; immediate "uses AWS" credibility |
| 2.2 | Spark (local) | **AWS Glue** or **EMR** | Glue = serverless/lower-ops; EMR = full Spark control |
| 2.3 | Kafka (self-run) | **Amazon MSK** or **Kinesis** | MSK = managed Kafka (same code); Kinesis = AWS-native alternative |
| 2.4 | HAPI FHIR | **AWS HealthLake** (optional) | Managed FHIR datastore; or keep HAPI on ECS/EC2 |
| 2.5 | docker-compose | **Terraform + IAM + VPC** | Infrastructure-as-code; the senior-level polish |

### Phase 2 deliverables

- [ ] **2.1** Spark writes Parquet to a real S3 bucket (change endpoint/creds only).
- [ ] **2.2** Spark job runs on Glue/EMR instead of local Spark.
- [ ] **2.3** Stream transport moved to MSK or Kinesis.
- [ ] **2.4** (Optional) FHIR source on HealthLake or HAPI-on-ECS.
- [ ] **2.5** Entire stack provisioned via Terraform; least-privilege IAM; resources in a VPC.
- [ ] **2.6** Cost guardrails: teardown scripts, AWS budget alert, free-tier/smallest instance types.
- [ ] **2.7** Basic observability: CloudWatch logs/metrics for the streaming job.

> ⚠️ **Cost caution:** MSK and EMR clusters bill while running and are easy to
> forget. Always tear down after a session, set a budget alert, and prefer the
> smallest instance types / free tier where possible.

### Resume framing

> "Real-time clinical vitals pipeline (FHIR → Kafka → Spark → S3). Built and
> tested locally with docker-compose; deployed to AWS (S3 + MSK/Kinesis +
> Glue/EMR) provisioned via Terraform with IAM/VPC and cost controls."

## Repo structure

```
clinical-monitoring-pipeline/
├── docker-compose.yml
├── requirements.txt
├── README.md
├── CLAUDE.md                # project context for Claude Code
├── src/
│   ├── load_to_fhir.py      # POST Synthea bundles into HAPI
│   ├── fhir_client.py       # paginated vital-signs retrieval from FHIR API
│   ├── producer.py          # FHIR API → Kafka
│   └── streaming_job.py     # Kafka → Spark → Parquet (+ flagging, windowing)
├── config/                  # vital-sign normal ranges, settings
├── tests/                   # pytest: flagging logic, data quality
└── data/                    # synthea output (gitignored)
```
