# Practical 9: Distributed SQL (Trino Coordinator Querying Across Heterogeneous Sources)

## Aim
To set up a **Trino (formerly PrestoSQL)** distributed SQL query engine coordinator using Docker and execute federated queries across multiple data sources (MySQL and In-Memory / Object Storage catalogs) simultaneously.

---

## Theory & Concept

### What is Trino?
Trino is a high-performance distributed SQL query engine designed for big data analytics. It does not store data itself; instead, it uses **Connectors** (configured in `/etc/trino/catalog/`) to query external data stores (e.g. MySQL, PostgreSQL, Hive, Delta Lake, AWS S3, Iceberg, and Kafka) in place using standard ANSI SQL.

### Federated Querying
A single SQL query in Trino can execute a join between a table stored in an RDBMS (MySQL) and a dataset stored in another catalog without moving or converting data beforehand:
```sql
SELECT * FROM mysql.university.students s
JOIN memory.default.courses c ON s.department = c.dept;
```

---

## Prerequisites
- Docker & Docker Compose installed and running.

---

## Step-by-Step Execution Instructions

### Step 1: Start MySQL and Trino Coordinator
```powershell
cd p9
docker compose up -d
```
Verify containers are up:
```powershell
docker compose ps
```
The Trino web UI is available at: `http://localhost:8080`.

### Step 2: Open Trino CLI Inside Container
Run the interactive Trino CLI:
```powershell
docker exec -it trino_coordinator trino
```

### Step 3: Run Distributed SQL Queries
Inside the Trino shell, run:
```sql
-- List available catalogs
SHOW CATALOGS;

-- Query MySQL students table
SELECT * FROM mysql.university.students;

-- Run aggregation
SELECT department, COUNT(*) AS student_count 
FROM mysql.university.students 
GROUP BY department;
```

### Step 4: Cleanup
```powershell
docker compose down
```

---

## Complete Source Codes

### 1. `docker-compose.yml`
```yaml
services:
  mysql:
    image: mysql:8.0
    container_name: trino_mysql
    environment:
      MYSQL_ROOT_PASSWORD: root123
      MYSQL_DATABASE: university
    ports:
      - "3307:3306"
    volumes:
      - ./mysql/init.sql:/docker-entrypoint-initdb.d/init.sql:ro

  trino:
    image: trinodb/trino:latest
    container_name: trino_coordinator
    ports:
      - "8080:8080"
    volumes:
      - ./etc/catalog:/etc/trino/catalog
    depends_on:
      - mysql
```

### 2. `mysql/init.sql`
```sql
CREATE TABLE IF NOT EXISTS university.students (
    student_id INT PRIMARY KEY,
    student_name VARCHAR(100),
    department VARCHAR(100)
);

INSERT INTO university.students (student_id, student_name, department) VALUES
(101, 'Aarav', 'Data Science'),
(102, 'Diya', 'Computer Science'),
(103, 'Rohan', 'Data Science'),
(104, 'Meera', 'Computer Science');

CREATE USER IF NOT EXISTS 'trino_user'@'%' IDENTIFIED BY 'trino123';
GRANT ALL PRIVILEGES ON university.* TO 'trino_user'@'%';
FLUSH PRIVILEGES;
```

### 3. `etc/catalog/mysql.properties`
```properties
connector.name=mysql
connection-url=jdbc:mysql://mysql:3306
connection-user=trino_user
connection-password=trino123
```

---

## Expected Output

```
trino> SHOW CATALOGS;
 Catalog 
---------
 memory  
 mysql   
 system  
(3 rows)

trino> SELECT * FROM mysql.university.students;
 student_id | student_name |   department     
------------+--------------+------------------
        101 | Aarav        | Data Science     
        102 | Diya         | Computer Science 
        103 | Rohan        | Data Science     
        104 | Meera        | Computer Science 
(4 rows)
```
