"""Central configuration for the clinical monitoring pipeline.

Endpoints, Kafka topic, MinIO/S3 credentials, vital-sign normal ranges,
and the LOINC-code -> vital-type mapping used to interpret FHIR Observations.
"""

# --- FHIR source ---
FHIR_BASE_URL = "http://localhost:8080/fhir"

# --- Kafka transport ---
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "clinical-vitals"

# --- MinIO / S3-compatible storage ---
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "clinical-data"

# --- Vital-sign normal ranges (used for flagging on Day 6) ---
# low/high are inclusive bounds; None means no bound on that side.
VITAL_RANGES = {
    "heart-rate":        {"low": 60,   "high": 100,  "unit": "beats/min"},
    "oxygen-saturation": {"low": 92,   "high": None, "unit": "%"},
    "body-temperature":  {"low": 36.1, "high": 37.2, "unit": "Cel"},
    "respiratory-rate":  {"low": 12,   "high": 20,   "unit": "breaths/min"},
}

# Severity tiers applied when a reading falls outside the normal range.
SEVERITY_LEVELS = ("normal", "warning", "critical")

# --- LOINC code -> canonical vital-type name (subset emitted by Synthea) ---
LOINC_TO_VITAL = {
    "8867-4":  "heart-rate",
    "2708-6":  "oxygen-saturation",
    "59408-5": "oxygen-saturation",
    "8310-5":  "body-temperature",
    "9279-1":  "respiratory-rate",
}
