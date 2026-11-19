# Practical 6: Flink State Lab (Fraud Detection using KeyedProcessFunction)

## Aim
To implement an Apache Flink stateful streaming application that detects **Fraudulent Transactions** (two large transactions $\ge \text{₹}10,000$ occurring within 1 minute / 60,000 ms for the same customer) using `KeyedProcessFunction` and `ValueState`.

---

## Theory & Concept

### Why Keyed State in Flink?
In streaming analytics, transactions arrive asynchronously. To detect multi-event patterns across time:
1. **Partition by Key (`keyBy(tx -> tx.customerId)`):** All transactions for a given customer are routed to the same operator instance.
2. **Managed Operator State (`ValueState<Transaction>`):** Remembers the customer's *last large transaction* across incoming events.
3. **Temporal Evaluation:** When a new transaction arrives:
   - Check if current amount $\ge \text{₹}10,000$.
   - Calculate time delta: $\Delta t = t_{\text{current}} - t_{\text{previous}}$.
   - If $\Delta t \le 60,000\text{ ms}$, emit a **Fraud Alert** to the output collector.
   - Update `ValueState` with the current transaction.

---

## Prerequisites
- Java 17+ (or Java 11)
- Apache Maven 3.6+
- (Optional) Python 3.8+ to run the included Python simulation.

---

## Step-by-Step Execution Instructions

### Method 1: Using Apache Maven (Java Flink)
1. Open PowerShell / Command Prompt.
2. Navigate to `p6`:
   ```powershell
   cd p6
   ```
3. Compile and execute using Maven:
   ```powershell
   mvn compile exec:exec
   ```
   *(Note: `exec:exec` is used instead of `exec:java` to prevent Maven in-process classloader isolation issues with Flink's MiniCluster).*

### Method 2: Using the Python Stream Simulator
```powershell
python fraud_detection.py
```

---

## Complete Source Codes

### 1. `pom.xml`
```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.example</groupId>
    <artifactId>flink-practical</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.release>17</maven.compiler.release>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <flink.version>1.19.2</flink.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-clients</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-simple</artifactId>
            <version>1.7.36</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.13.0</version>
            </plugin>
            <plugin>
                <groupId>org.codehaus.mojo</groupId>
                <artifactId>exec-maven-plugin</artifactId>
                <version>3.1.0</version>
                <configuration>
                    <executable>java</executable>
                    <mainClass>com.example.FraudDetectionJob</mainClass>
                    <arguments>
                        <argument>-classpath</argument>
                        <classpath />
                        <argument>com.example.FraudDetectionJob</argument>
                    </arguments>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

### 2. `src/main/java/com/example/FraudDetectionJob.java`
```java
package com.example;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.time.Instant;

public class FraudDetectionJob {

    public static class Transaction implements Serializable {
        public String customerId;
        public double amount;
        public long timestamp;

        public Transaction() {}

        public Transaction(String customerId, double amount, long timestamp) {
            this.customerId = customerId;
            this.amount = amount;
            this.timestamp = timestamp;
        }
    }

    public static class FraudAlert implements Serializable {
        public String customerId;
        public double previousAmount;
        public double currentAmount;
        public long gapMs;

        public FraudAlert() {}

        public FraudAlert(String customerId, double previousAmount, double currentAmount, long gapMs) {
            this.customerId = customerId;
            this.previousAmount = previousAmount;
            this.currentAmount = currentAmount;
            this.gapMs = gapMs;
        }

        @Override
        public String toString() {
            return "FRAUD ALERT: Customer=" + customerId
                    + ", Previous=₹" + previousAmount
                    + ", Current=₹" + currentAmount
                    + ", Gap=" + (gapMs / 1000) + " seconds";
        }
    }

    public static class FraudDetector extends KeyedProcessFunction<String, Transaction, FraudAlert> {
        private transient ValueState<Transaction> previousTransaction;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            ValueStateDescriptor<Transaction> descriptor =
                    new ValueStateDescriptor<>(
                            "previous-large-transaction",
                            Types.POJO(Transaction.class)
                    );
            previousTransaction = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(
                Transaction tx,
                Context ctx,
                Collector<FraudAlert> out) throws Exception {

            if (tx.amount < 10000) {
                return;
            }

            Transaction previous = previousTransaction.value();
            if (previous != null) {
                long gap = tx.timestamp - previous.timestamp;
                if (gap >= 0 && gap <= 60000) {
                    out.collect(new FraudAlert(
                            tx.customerId,
                            previous.amount,
                            tx.amount,
                            gap
                    ));
                }
            }

            previousTransaction.update(tx);
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        long base = Instant.parse("2026-10-19T10:00:00Z").toEpochMilli();

        DataStream<Transaction> transactions = env.fromData(
                new Transaction("C101", 15000, base + 10000),
                new Transaction("C202", 5000,  base + 20000),
                new Transaction("C101", 20000, base + 45000),
                new Transaction("C202", 12000, base + 50000),
                new Transaction("C101", 3000,  base + 55000),
                new Transaction("C101", 18000, base + 90000),
                new Transaction("C202", 14000, base + 130000)
        );

        transactions
                .keyBy(tx -> tx.customerId)
                .process(new FraudDetector())
                .print();

        env.execute("Fraud Detection Job");
    }
}
```

---

## Expected Output

```
> FRAUD ALERT: Customer=C101, Previous=₹15000.0, Current=₹20000.0, Gap=35 seconds
> FRAUD ALERT: Customer=C101, Previous=₹20000.0, Current=₹18000.0, Gap=45 seconds
[INFO] BUILD SUCCESS
```
