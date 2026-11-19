# Practical 4: Kafka Schema Evolution (Backward Compatible Schema Change)

## Aim
To demonstrate schema management and **Backward Compatible** schema evolution using the **Confluent Kafka Schema Registry** with Apache Avro.

---

## Theory & Concept

### What is Schema Evolution?
In distributed streaming architectures, producer and consumer applications evolve independently. The Schema Registry acts as a centralized governance layer enforcing compatibility rules:
- **BACKWARD Compatibility (Default):** Consumers using the *new* schema (e.g. V2) can read data produced with the *previous* schema (e.g. V1).
  - *Rule:* Deleting fields or adding **optional fields with a default value** is backward-compatible.
  - *Incompatible:* Adding a required field without a default value or changing data types (e.g. `int` to `string`).
- When an incompatible schema (V3) is registered, the Schema Registry rejects it with an **HTTP 409 Conflict**.

---

## Prerequisites
- Docker & Docker Compose installed and running.
- Python 3.8+ (no third-party pip dependencies required; uses Python standard library `urllib`).

---

## Step-by-Step Execution Instructions

### Step 1: Start Kafka & Schema Registry
```powershell
cd Practical_04_Kafka_Schema_Evolution
docker compose up -d
```
Check status:
```powershell
docker compose ps
```
Ensure `kafka` (port 9092) and `schema-registry` (port 8081) are running.

### Step 2: Run the Schema Evolution Test Suite
```powershell
python practical4.py
```
This tests:
1. Registering V1 (`patient_id`, `patient_name`, `age`, `heart_rate`).
2. Setting compatibility mode to `BACKWARD`.
3. Validating and registering V2 (adds `blood_pressure` with default `"Unknown"` -> ACCEPTED).
4. Validating V3 (incompatible changes -> REJECTED with HTTP 409).
5. Listing active versions `[1, 2]`.

### Step 3: Produce and Consume Avro Records
Run the PowerShell script:
```powershell
.\produce_consume.ps1
```
(Or on Linux/macOS: `bash produce_consume.sh`)

### Step 4: Cleanup
```powershell
docker compose down
```

---

## Expected Output (`python practical4.py`)

```
======================================================================
STEP 1: Wait for Schema Registry
======================================================================
Registry is up. Existing subjects: []

======================================================================
STEP 2: Register V1 (patient_id, patient_name, age, heart_rate)
======================================================================
(200, {'id': 1})

======================================================================
STEP 3: Set compatibility of subject to BACKWARD
======================================================================
(200, {'compatibility': 'BACKWARD'})
Current config: {'compatibilityLevel': 'BACKWARD'}

======================================================================
STEP 4: Check V2 (adds blood_pressure WITH default) for compatibility
======================================================================
(200, {'is_compatible': True})

======================================================================
STEP 5: Register V2
======================================================================
(200, {'id': 2})
-> V2 ACCEPTED

======================================================================
STEP 6: Check incompatible V3 (age int->string, new required field w/o default)
======================================================================
(200, {'is_compatible': False, 'messages': [...]})

======================================================================
STEP 7: Try to register V3
======================================================================
HTTP 409
{
  "error_code": 409,
  "message": "Schema being registered is incompatible with an earlier schema for subject \"patient-data-value\"..."
}
-> V3 REJECTED

======================================================================
STEP 8: Verify all registered versions
======================================================================
Versions: [1, 2]
```
