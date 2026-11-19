-- 1. Verify Catalogs
SHOW CATALOGS;

-- 2. Query MySQL Students Table via Trino
SELECT * FROM mysql.university.students;

-- 3. Aggregation across Department
SELECT 
    department, 
    COUNT(*) AS total_students 
FROM mysql.university.students 
GROUP BY department;

-- 4. Federated Cross-Catalog Query (Join MySQL with In-Memory Catalog)
CREATE SCHEMA IF NOT EXISTS memory.default;
CREATE TABLE IF NOT EXISTS memory.default.courses (
    course_name VARCHAR,
    dept VARCHAR
);

INSERT INTO memory.default.courses VALUES
('Machine Learning', 'Data Science'),
('Operating Systems', 'Computer Science');

-- Federated Join across MySQL and Memory catalogs:
SELECT 
    s.student_id,
    s.student_name,
    s.department,
    c.course_name
FROM mysql.university.students s
JOIN memory.default.courses c
ON s.department = c.dept;
