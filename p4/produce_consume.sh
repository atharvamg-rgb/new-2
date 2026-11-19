#!/usr/bin/env bash
# Produce V2 patient records and read them back (run AFTER practical4.py)
set -e
SCHEMA=$(tr -d '\n' < schemas/patient-v2.avsc)

docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic patient-data --partitions 1 --replication-factor 1

echo '{"patient_id":101,"patient_name":"Amit","age":45,"heart_rate":78,"blood_pressure":"120/80"}
{"patient_id":102,"patient_name":"Neha","age":38,"heart_rate":82,"blood_pressure":"118/76"}' | \
docker exec -i schema-registry kafka-avro-console-producer \
  --bootstrap-server kafka:29092 --topic patient-data \
  --property schema.registry.url=http://localhost:8081 \
  --property value.schema="$SCHEMA"

echo "----- consuming -----"
docker exec schema-registry kafka-avro-console-consumer \
  --bootstrap-server kafka:29092 --topic patient-data --from-beginning --max-messages 2 \
  --property schema.registry.url=http://localhost:8081
