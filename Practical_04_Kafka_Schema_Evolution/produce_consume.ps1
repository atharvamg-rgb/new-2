# Produce V2 patient records and read them back (PowerShell). Run AFTER practical4.py
$ErrorActionPreference = "Stop"

Write-Host "`n=== Creating topic patient-data ===" -ForegroundColor Cyan
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic patient-data --partitions 1 --replication-factor 1

Write-Host "`n=== Copying V2 schema and patient records into the container ===" -ForegroundColor Cyan
docker cp schemas/patient-v2.avsc schema-registry:/tmp/patient-v2.avsc
docker cp patients.json schema-registry:/tmp/patients.json

Write-Host "`n=== Producing patient data with the V2 schema ===" -ForegroundColor Cyan
docker exec schema-registry sh -c "kafka-avro-console-producer --bootstrap-server kafka:29092 --topic patient-data --property schema.registry.url=http://localhost:8081 --property value.schema.file=/tmp/patient-v2.avsc < /tmp/patients.json"
Write-Host "Produced 2 records." -ForegroundColor Green

Write-Host "`n=== Consuming patient data ===" -ForegroundColor Cyan
docker exec schema-registry kafka-avro-console-consumer --bootstrap-server kafka:29092 --topic patient-data --from-beginning --max-messages 2 --timeout-ms 15000 --property schema.registry.url=http://localhost:8081
