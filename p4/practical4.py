"""Practical 4 - Schema Registry BACKWARD compatibility (stdlib only)."""
import json
import time
import urllib.request
import urllib.error

REG = "http://localhost:8081"
SUBJECT = "patient-data-value"
CT = {"Content-Type": "application/vnd.schemaregistry.v1+json"}


def call(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(REG + path, data=data, method=method, headers=CT)
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, json.loads(r.read() or "null")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or "null")


def schema_body(path):
    with open(path) as f:
        return {"schema": json.dumps(json.load(f))}


def step(n, title):
    print(f"\n{'='*70}\nSTEP {n}: {title}\n{'='*70}")


# Step 1: Wait for Schema Registry
step(1, "Wait for Schema Registry")
for _ in range(30):
    try:
        status, subjects = call("GET", "/subjects")
        print("Registry is up. Existing subjects:", subjects)
        break
    except Exception:
        time.sleep(2)

call("DELETE", f"/subjects/{SUBJECT}")  # soft delete (clean re-runs)
call("DELETE", f"/subjects/{SUBJECT}?permanent=true")

# Step 2: Register V1
step(2, "Register V1 (patient_id, patient_name, age, heart_rate)")
print(call("POST", f"/subjects/{SUBJECT}/versions", schema_body("schemas/patient-v1.avsc")))

# Step 3: Set compatibility to BACKWARD
step(3, "Set compatibility of subject to BACKWARD")
print(call("PUT", f"/config/{SUBJECT}", {"compatibility": "BACKWARD"}))
current_cfg = call("GET", f"/config/{SUBJECT}")
print("Current config:", current_cfg[1])

# Step 4: Check V2 compatibility
step(4, "Check V2 (adds blood_pressure WITH default) for compatibility")
print(call("POST", f"/compatibility/subjects/{SUBJECT}/versions/latest", schema_body("schemas/patient-v2.avsc")))

# Step 5: Register V2
step(5, "Register V2")
status, resp = call("POST", f"/subjects/{SUBJECT}/versions", schema_body("schemas/patient-v2.avsc"))
print((status, resp))
print("-> V2 ACCEPTED")

# Step 6: Check incompatible V3
step(6, "Check incompatible V3 (age int->string, new required field w/o default)")
print(call("POST", f"/compatibility/subjects/{SUBJECT}/versions/latest", schema_body("schemas/patient-v3-incompatible.avsc")))

# Step 7: Try to register V3
step(7, "Try to register V3")
status, resp = call("POST", f"/subjects/{SUBJECT}/versions", schema_body("schemas/patient-v3-incompatible.avsc"))
print(f"HTTP {status}\n{json.dumps(resp, indent=2)}")
print("-> V3 REJECTED")

# Step 8: Verify all registered versions
step(8, "Verify all registered versions")
status, versions = call("GET", f"/subjects/{SUBJECT}/versions")
print("Versions:", versions)

for v in versions:
    _, data = call("GET", f"/subjects/{SUBJECT}/versions/{v}")
    print(f"\n-- version {data.get('version')} (id={data.get('id')}) --")
    print(json.dumps(json.loads(data["schema"]), indent=2))
