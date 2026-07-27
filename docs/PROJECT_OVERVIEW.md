# Project Overview — Real-Time Clinical Vital-Signs Monitoring Pipeline

A beginner-friendly explanation of what this project is, why it's built this way,
which tools were chosen and why, whether it reflects real-world systems, and how
it fits a data-engineering job search.

---

## Brief intro for interviews

> I built a real-time data pipeline that ingests patient vital signs from a FHIR
> medical-records server, streams them through Kafka, processes them with Spark
> Structured Streaming, and lands them as Parquet in an S3-compatible data lake.
> The goal is **early detection of patient deterioration** — catching patients who
> are trending toward a crisis (like sepsis) *before* it happens.
>
> The key distinction is from the **bedside monitor**: a bedside alarm fires when a
> single value crosses a threshold *right now*, on *one* patient. But the deadliest
> events build up *gradually over hours* — heart rate, respiratory rate, and
> temperature each drifting while still looking "in range," so no instantaneous
> alarm trips until it's too late. My pipeline exists to do what the bedside box
> structurally can't: **track trends over time, look across all patients at once,
> and store the full history** so you can flag gradual deterioration, give a rapid
> response team a hospital-wide risk picture, retune noisy alarms, and train
> predictive models.
>
> It's deliberately built on the in-demand open-source stack (Kafka, Spark, Parquet,
> Docker), all runnable locally, with a documented path to deploy the same design on
> AWS. So it's not a replacement for the certified bedside alarm — it's the
> **analytics and storage backbone that sits behind it.**

*(For the "is storing all this data even worth it?" pushback, see §5a below — that's
the strongest justification and a good thing to be ready to defend.)*

---

## 1. What problem does it solve?

In a hospital, patients are monitored for **vital signs**: heart rate, blood
oxygen (SpO₂), body temperature, and respiratory rate. A single bedside monitor
that shows one patient's numbers and beeps on danger is a *solved* problem —
simple, dedicated hardware. **This project is the layer behind that**, answering
harder questions the bedside box can't:

- Show **every** patient trending toward danger across the whole hospital, right now.
- **Store** every reading from every patient for later analysis and ML.
- Pull from **many** monitor brands + the medical-records system into **one**
  standardized stream.

That "collect everything continuously, flag the dangerous readings, store it all
for analysis" job is a **data engineering** problem. This pipeline does it in
miniature:

> Read vital signs from a medical-records system → flag abnormal readings
> (e.g. heart rate > 100) → save everything in an organized, queryable format.

## 2. The big picture — what is a "data pipeline"?

A data pipeline is an assembly line for data: raw data enters one end, passes
through stations that clean/transform/enrich it, and exits in a useful form.

```
HAPI FHIR  →  Kafka  →  Spark  →  MinIO (Parquet files)
 (source)    (buffer)  (process)   (storage / "data lake")
```

## 3. Each tool, from scratch

### HAPI FHIR — the data source
- **FHIR** ("fire") = the healthcare industry's standard format for sharing
  patient data — a universal language for medical records. A vital-sign reading
  is an **Observation**, tagged with a **LOINC** code (e.g. `8867-4` = heart rate).
- **HAPI** = a free, open-source FHIR *server*; stores the data and serves it over
  a REST API (`GET /fhir/Observation?...`). Runs locally in a container.
- Real patient data is legally protected (HIPAA), so we generate realistic *fake*
  patients with **Synthea** and load them into HAPI — authentic shape, no real PHI.
- **Why it matters:** using FHIR (not a CSV) signals you understand how real
  healthcare systems actually exchange data.

### Kafka — the streaming buffer
- **Problem it solves:** data is produced continuously, and the consumer may be
  slower or may crash. We need nothing lost and the two sides decoupled.
- **Mailbox analogy:** instead of hand-delivering each letter and waiting at the
  door, you drop it in a mailbox; the recipient reads it whenever ready. Kafka is
  that mailbox for data.
- Producers write messages to a **topic** (`clinical-vitals`); consumers (Spark)
  read at their own pace; Kafka keeps messages on disk so they survive a crash.
- **Payoffs:** no data loss (consumer resumes from its offset), absorbs bursts
  (backpressure), decoupling (sides run independently), replay + fan-out.
- **"Decouple"** = let two parts work independently instead of depending on each
  other's timing.
- **KRaft mode**: Kafka now manages its own metadata, so the old helper service
  **Zookeeper** is no longer needed — one fewer moving part.

### Spark — the processing brain
- **Apache Spark** processes large data; we use **Structured Streaming** to process
  data *as it arrives*.
- **Batch vs streaming:** batch = process a fixed pile once (Sunday laundry);
  streaming = process continuously as data arrives (wash each dish as it's dirtied).
- Spark streaming uses **micro-batching**: every few seconds it grabs new messages,
  processes that small chunk like a mini-batch, writes output, repeats. Feels
  continuous; really a fast loop of tiny batches — so streaming code ≈ batch code.
- **Our job:** read Kafka → parse JSON `{patient, vital_type, value, unit, timestamp}`
  → flag abnormal readings against normal ranges → write Parquet, and **checkpoint**
  its stream position so it resumes cleanly after a restart.

### MinIO — the storage / data lake
- A **data lake** is cheap bulk file storage. The cloud standard is **Amazon S3**
  (objects in buckets). **MinIO** behaves exactly like S3 but runs locally and free
  — a drop-in stand-in. Our bucket: `clinical-data`.
- We store **Parquet**: a columnar (column-by-column) format that makes analytical
  queries fast and files small. The default for data lakes.

### Docker / docker-compose — the wrapper
- **Containers** package a program with everything it needs so it runs the same
  anywhere. **Docker** runs them; **docker-compose** describes *all* the containers
  in one file and starts them together (`docker compose up`). That's Day 1's
  deliverable.

## 4. Why designed this way?

- **Source → Buffer → Process → Store** is the canonical streaming architecture,
  seen (with cloud-branded tools) at most data-driven companies.
- **Built one station at a time** (the 10-day plan) so each piece is verified before
  the next is added — never debug four things at once.
- Every tool is **free + local** *and* the open-source twin of what companies pay
  for in the cloud, so skills transfer directly:

| Local tool | Cloud equivalent |
|---|---|
| MinIO | Amazon S3 |
| Kafka | AWS MSK / Amazon Kinesis |
| Spark | AWS EMR / Glue / Databricks |
| HAPI FHIR | AWS HealthLake |
| docker-compose | Terraform / Kubernetes |

This is why **Phase 2** ports the whole thing to AWS (see README).

## 5. Is it used in real life? By whom?

Yes — a real, active category: **patient deterioration detection** / **early
warning scoring** (NEWS2 / MEWS). Patients often show abnormal vitals *hours*
before a crisis, so catching the trend early saves lives. **Remote Patient
Monitoring (RPM)** (home wearables streaming to clinicians) is booming.

**Who builds it:** device/platform makers (Philips, GE HealthCare, Medtronic),
specialized health-analytics vendors (Epic "Deterioration Index", Oracle/Cerner,
Etiometry, Bernoulli, Current Health), and large hospital systems' internal data
teams. Hospitals mostly **buy/partner** rather than build.

**Still needed?** Yes — alarm fatigue (too many alerts → real ones missed) and
interoperability are unsolved, and healthcare is actively modernizing from slow
batch systems to real-time streaming. We build the **analytics/storage backbone**
behind the certified bedside alarm — not a replacement for it.

## 5a. "Why store and analyze all this data — isn't the bedside beep enough?"

A fair challenge: if a bedside monitor already flags abnormal values and beeps, why
build a whole pipeline to store and analyze everything? Honest answer: **for one
stable patient in the moment, the bedside beep IS enough — the data layer would be
overkill.** Its value comes from three things the bedside box structurally cannot
do: see **trends over time**, see **across many patients**, and **learn from
history**. Where those matter, it flips from "nice to have" to essential:

1. **Trends catch what thresholds miss (the killer argument).** The deadliest events
   build *gradually*. **Sepsis** (~270k US deaths/year, the most expensive inpatient
   condition) shows up as a subtle *combined drift* — HR, respiratory rate, and temp
   all creeping over hours while each stays "in range," so no instantaneous alarm
   trips until it's almost too late. Detecting it requires storing the time-series
   and analyzing the multi-vital trend — exactly what this pipeline enables. Early
   warning scoring (NEWS2/MEWS) is now a regulatory expectation for this reason.
2. **One bed vs. the whole hospital.** The bedside knows only its bed. "Which of our
   600 patients are deteriorating right now, ranked by risk?" (rapid response team
   dispatch) and "is Ward 4 getting slammed tonight?" (staffing) require aggregating
   *all* streams into one place.
3. **You can't fix alarm fatigue without the data.** Bedside alarms fire so often
   (mostly false) that staff tune them out and miss real ones — a documented safety
   crisis. Fixing it means analyzing months of historical alarm + vitals data to see
   which alarms actually predicted bad outcomes, then retuning. That needs stored,
   queryable history.
4. **Boring-but-mandatory.** The medical record is a legal document with retention
   requirements; **Remote Patient Monitoring is only billable if the data is captured
   and stored** (Medicare requires 16+ days/month); quality/accreditation reporting
   is all downstream of stored data.
5. **You can't train a model on data you threw away.** Every ML deterioration/sepsis
   predictor was trained on historical stored vitals. Beep-and-discard makes that
   impossible.

**Calibrated takeaway (good interview line):** "For instantaneous single-patient
alerting, the bedside monitor is sufficient and this pipeline would be overkill. The
data layer becomes essential the moment you need trend-based detection, cross-patient
situational awareness, regulatory/billing data capture, or any ML — and its value
scales with the size, acuity, and remote-monitoring footprint of the organization."
Showing you know when *not* to build it is what sounds senior.

**Real-world reference (worth reading + name-dropping):** *"How Data Science Can
Drive Care for Sepsis and Patient Deterioration"* — University of Maryland Medical
System / iHarbor, co-written by a hospital CMO and a healthcare data-science
director (umms.org/iharbor). It confirms the whole premise: sepsis ≈ 2M cases /
~300k deaths / $52B inpatient cost a year (2021); today's rule-based **Early Warning
Scores** are "blunt instruments" that *detect rather than predict* and cause alarm
fatigue; the future is **ML that predicts earlier** by learning *temporal,
multi-variable* patterns ("a heart rate rising 20 BPM over 3 hours tells a different
story than one simply elevated"), trained and **validated locally**, with MLOps-style
calibration and monitoring. In our project terms: the rule-based flagging is the EWS
baseline, and the optional **Phase 4 (ML scoring & MLOps)** is exactly that upgrade.
⚠️ Honesty caveat for interviews: Synthea data is synthetic, so any model here
demonstrates the *engineering* (feature pipeline, in-stream scoring, MLflow tracking),
not a clinically valid predictor.

## 6. Should we add Airflow?

**Not into the streaming path.** Airflow is an **orchestrator** — it schedules and
sequences *batch* jobs ("every night run A → B → C"). This pipeline is
**streaming** (continuous, event-driven), so the two solve different problems, and
forcing Airflow into the live stream would be conceptually wrong (and obvious to a
sharp interviewer).

**Where it fits honestly:** orchestrating the *batch* side-tasks that complement
the stream — nightly Synthea generation, data-quality reports, Parquet
compaction/cleanup, daily aggregate rollups, model retraining. A common real
architecture is **streaming for the live path + Airflow for scheduled batch**, side
by side. That's the planned **Phase 3** — added as a separate, clearly-scoped
batch layer, not shoehorned into the stream. Knowing *when* to use each tool is
more impressive than listing both.

## 7. Is it helpful for the job search?

**Yes — genuinely.**

- Hits the exact in-demand stack: **Kafka, Spark Structured Streaming, S3/Parquet,
  Docker** — among the most-requested DE skills.
- It's an **end-to-end system**, not a toy script — wiring a whole pipeline together
  is far more convincing than isolated exercises.
- The **healthcare/FHIR** angle is a differentiator for a large, growing sector.
- Produces real **interview stories** (e.g. the distroless-image healthcheck bug).
- Demonstrates **judgment** (streaming vs batch, KRaft vs Zookeeper, *not* adding
  Airflow blindly) — interviewers probe for exactly this.

**Caveats (honest):** a portfolio project *supports*, not replaces, fundamentals
(SQL, Python, data modeling) and applying widely. Its value is unlocked by your
ability to **explain every box in the diagram in plain English** — so keep doing
that. Phase 2 (AWS + Terraform) meaningfully raises its perceived seniority.

**Bottom line:** a credible, in-demand project targeting a real hiring market.
Build it, deploy it to the cloud, write a clear README, and be able to explain it
end to end.

## 8. Streaming vs. batch, and what's a "service" (common confusion)

**The misconception:** that the pipeline runs *sequentially* — HAPI finishes, then
Kafka runs, then Spark, like a relay race. It doesn't. That "one-finishes-then-the-
next-starts" pattern is **batch**; this project is **streaming**.

**The correct picture — an always-on assembly line:** HAPI, Kafka, Spark, and MinIO
all start up and then run **continuously and in parallel, forever.** A single
vital-sign reading flows *through* them in order (HAPI → Kafka → Spark → MinIO), but
the programs don't take turns — while reading #1 is in Spark, #2 is sitting in Kafka,
and #3 is arriving from HAPI. The *data* has a sequence; the *programs* are all alive
at once. (Like a car factory: paint/weld/assembly stations are all staffed all day;
different cars sit at different stations simultaneously.)

**This is exactly why separate containers make sense:** because all four run
continuously and concurrently, each needs its own isolated, always-running,
independently-restartable home.

**Batch (the other style)** = Job A runs to completion, *then* B starts on A's output,
*then* C. Sequential, scheduled, with a clear start/end. That's what **Airflow**
(Phase 3) orchestrates. We saw a tiny taste of batch on Day 1: the `minio-init`
container ran once (created the bucket), then exited — unlike the three long-running
services.

**What is a "service"?** A program that **starts and then keeps running indefinitely,
waiting to handle work**, instead of running once and exiting. Defining trait: it
doesn't finish on its own — you stop it.

| | Service (long-running) | Batch job / script (run-once) |
|---|---|---|
| Lifecycle | Starts, stays up indefinitely | Runs, completes, exits |
| Finishes itself? | No — you stop it | Yes — when done |
| Examples here | HAPI, Kafka, Spark stream, MinIO | `minio-init`, a one-off script |
| Analogy | A shop open all day | A one-time delivery |

That's why `docker-compose` uses the keyword `services:` — it declares the set of
long-running programs to keep alive together. (Loose usage note: compose calls *every*
entry a "service" even though `minio-init` is really a run-once job; and in cloud talk
"service" can mean a whole product like "AWS S3." Core technical meaning = a
continuously-running program waiting to handle work.)

**One-liners for interviews:**
- *Streaming:* the services run continuously and in parallel; the data flows through
  them in sequence — an always-on assembly line, nothing "finishes."
- *Service:* a program that starts and keeps running indefinitely to handle work,
  rather than running once and exiting.
