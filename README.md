# Governance Demo
**End-to-End Data Governance with a Restricted PII Activation Path (Local-first, AWS-mappable)**

This repo is a **fully runnable** local data platform that demonstrates practical data governance end-to-end:

```
ingestion → raw → clean → curated → serving
                       ↘
                        restricted_pii → activation_exports
```

It covers both:
- **analytics governance** (curated facts, serving tables)
- **controlled operational PII usage** (activation exports) with explicit roles + audit evidence

---

## Quickstart

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -U pip setuptools wheel
pip install -e .
```

Initialize:

```bash
export GOVDEMO_ROLE=data_engineer
export PII_TOKEN_SECRET="dev-secret-change-me"
govdemo init
```

Run the full demo:

```bash
./scripts/demo.sh
```

---

## Local layout (simulating AWS)

```text
data_lake/
  landing/            # untrusted input
  raw/                # immutable, append-only
  clean/              # validated + PII tokenized
  curated/            # analytics facts (no raw PII)
  serving/            # product contracts
  restricted_pii/     # identity/PII zone (restricted)
  exports/            # governed exports for operations
  quarantine/         # invalid input

warehouse/
  governance.duckdb   # audit_runs, gdpr_requests, activation_exports
  lineage.jsonl       # lineage edges
  gdpr_evidence/      # GDPR evidence artifacts
  export_evidence/    # export evidence artifacts
```

---

## Governance principles implemented

### 1) PII is handled **before** GDPR (raw → clean boundary)
- Raw may contain email / phone / IP (restricted access)
- Clean replaces raw PII with tokens (`email_token`, `ip_token`)
- Curated/serving never contain raw PII

### 2) Operational PII use is explicit and auditable
Analytics doesn’t need raw PII. Operational workflows sometimes do.
This repo models that with:
- a **restricted_pii identity table** (`user_id → email`)
- a controlled **activation export** that joins curated audience → restricted identity
- audit logs + evidence files for every export

### 3) Raw is immutable
Deletes propagate downstream (clean/curated/serving/restricted_pii), while raw stays audit-grade.

---

## Role × Layer × Permission matrix

```text
| Role               | Raw | Clean | Curated | Serving | Restricted PII | Exports | Quarantine | Audit |
|--------------------|-----|-------|---------|---------|----------------|---------|------------|-------|
| data_engineer      | R/W | R/W   | R/W     | R/W     | R/W            | R/W     | R/W        | R/W   |
| analyst            |  -  |  -    |  R      |  -      |  -             |  -      |  -         |  R    |
| product_service    |  -  |  -    |  -      |  R      |  -             |  -      |  -         |  -    |
| activation_service |  -  |  -    |  R      |  -      |  R             |  W      |  -         |  R    |
| governance_officer |  -  |  -    |  -      |  -      |  -             |  -      |  -         |  R    |
```

R=read, W=write, -=no access

---

## Inspect data quickly

```bash
# raw (jsonl)
head -n 5 data_lake/raw/events/dt=*/source=*/part-*.jsonl

# clean / curated / serving / restricted PII (parquet)
python -c "import pyarrow.parquet as pq; print(pq.ParquetFile('data_lake/clean/events/dt=*/part-00001.parquet').read().to_pandas().head())"
python -c "import pyarrow.parquet as pq; print(pq.ParquetFile('data_lake/curated/facts/dt=*/fact_user_activity_daily.parquet').read().to_pandas())"
python -c "import pyarrow.parquet as pq; print(pq.ParquetFile('data_lake/serving/user_metrics/dt=*/user_metrics.parquet').read().to_pandas())"
python -c "import pyarrow.parquet as pq; print(pq.ParquetFile('data_lake/restricted_pii/identity/dt=*/identity.parquet').read().to_pandas())"
```

---

## Activation export (controlled PII usage)

The activation export joins:
- **curated facts** (audience definition)  
with
- **restricted_pii identity** (PII resolution)

Example:

```bash
export GOVDEMO_ROLE=activation_service
govdemo export-audience --min-events 2
```

Output:
- CSV in `data_lake/exports/audience/dt=YYYY-MM-DD/audience.csv`
- audit record + evidence JSON in `warehouse/export_evidence/`

---

## GDPR delete

```bash
export GOVDEMO_ROLE=data_engineer
govdemo gdpr request --user-id u1 --mode delete
```

Deletes propagate to:
- clean
- curated
- serving
- restricted_pii

Raw remains immutable, and evidence is recorded.

---

## AWS mapping

```text
| Local Prefix / Component     | AWS Equivalent                                  |
|-----------------------------|--------------------------------------------------|
| data_lake/*                 | S3 bucket + prefixes                             |
| restricted_pii/*            | S3 restricted prefix + Lake Formation + KMS      |
| exports/*                   | S3 exports prefix (encrypted) + IAM-scoped role  |
| audit tables (DuckDB)       | DynamoDB / Redshift audit schema                 |
| lineage.jsonl               | OpenLineage + DataHub                            |
| pipelines (local)           | Glue jobs / ECS tasks / Step Functions           |
```
