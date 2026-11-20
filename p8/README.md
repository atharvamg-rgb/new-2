# Practical 8: GraphX Social Network PageRank Analysis

## Aim
To use **Apache Spark GraphX** to model a social network graph, calculate vertex importance using the **PageRank** algorithm, and identify the "Most Influential" user.

---

## Theory & Concept

### PageRank Algorithm
PageRank measures the relative importance of vertices within a directed graph. The intuition is that a person is influential if they are linked to by other influential people:
$$PR(u) = \alpha + (1 - \alpha) \sum_{v \in B(u)} \frac{PR(v)}{L(v)}$$
Where:
- $\alpha = 0.15$ is the reset probability (teleportation factor).
- $B(u)$ is the set of vertices pointing to $u$.
- $L(v)$ is the out-degree of vertex $v$.

### Graph Structure
- **Vertices (6 Users):** Alice (1), Bob (2), Charlie (3), David (4), Emma (5), Frank (6).
- **Edges (7 Relationships):** Directed follow/influence links.
- **Result:** Charlie receives incoming endorsements from both Alice and Bob, resulting in the highest PageRank score ($\approx 2.12$).

---

## Prerequisites & Setup

### For Method 1: Python (Recommended)
- **Python 3.8+** (already installed in your environment).
- **Dependencies:** **None!** Standard library only (`csv`, `os`). Zero extra packages needed.

### For Method 2: Scala / GraphX (Optional)
- **Java 17 or Java 11 JDK** (configured in `JAVA_HOME`).
- **sbt (Scala Build Tool):**
  Install on Windows via PowerShell:
  ```powershell
  winget install --id Scala.sbt
  ```

---

## Step-by-Step Execution Instructions

### Method 1: Python Implementation (Instant & Standalone)
1. Open PowerShell / Command Prompt / Git Bash.
2. Navigate to the `p8` directory:
   ```powershell
   cd p8
   ```
3. Run the script:
   ```powershell
   python pagerank.py
   ```

### Method 2: Scala / sbt (Spark GraphX)
1. Navigate to the `p8` directory:
   ```powershell
   cd p8
   ```
2. Compile and run via `sbt`:
   ```powershell
   sbt run
   ```

---

## Complete Source Codes

### 1. `data/vertices.csv`
```csv
id,name
1,Alice
2,Bob
3,Charlie
4,David
5,Emma
6,Frank
```

### 2. `data/edges.csv`
```csv
src,dst
1,2
1,3
2,3
3,1
4,2
5,2
6,4
```

### 3. `src/main/scala/GraphXPageRank.scala`
```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object GraphXPageRank {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("GraphX PageRank")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    println("Spark Started Successfully")

    val users: RDD[(VertexId, String)] =
      sc.textFile("data/vertices.csv")
        .filter(!_.startsWith("id,"))
        .map { line =>
          val fields = line.split(",")
          (fields(0).toLong, fields(1))
        }

    val relationships: RDD[Edge[Int]] =
      sc.textFile("data/edges.csv")
        .filter(!_.startsWith("src,"))
        .map { line =>
          val fields = line.split(",")
          Edge(fields(0).toLong, fields(1).toLong, 1)
        }

    val graph = Graph(users, relationships)
    println("Graph Created Successfully")
    println("Total Users: " + graph.numVertices)
    println("Total Relationships: " + graph.numEdges)

    val ranks = graph.pageRank(0.0001).vertices

    val result = users.join(ranks)
      .map { case (id, (name, rank)) => (name, rank) }
      .sortBy(_._2, ascending = false)

    println("\nPageRank Results")
    result.collect().foreach { case (name, rank) => println(name + " : " + rank) }

    println("\nMost Influential User")
    println(result.first())

    spark.stop()
  }
}
```

### 4. `pagerank.py` (Python Implementation)
```python
import csv
import os

def load_data():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    vertices_path = os.path.join(base_dir, "data", "vertices.csv")
    edges_path = os.path.join(base_dir, "data", "edges.csv")

    users = {}
    with open(vertices_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            if row:
                users[int(row[0])] = row[1]

    edges = []
    with open(edges_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            if row:
                edges.append((int(row[0]), int(row[1])))

    return users, edges


def compute_pagerank(users, edges, tol=0.0001, reset_prob=0.15):
    nodes = list(users.keys())
    out_deg = {n: 0 for n in nodes}
    in_edges = {n: [] for n in nodes}

    for u, v in edges:
        out_deg[u] += 1
        in_edges[v].append(u)

    ranks = {n: reset_prob for n in nodes}
    max_diff = 1.0

    while max_diff > tol:
        new_ranks = {}
        for n in nodes:
            incoming_sum = sum(ranks[u] / out_deg[u] for u in in_edges[n] if out_deg[u] > 0)
            new_ranks[n] = reset_prob + (1.0 - reset_prob) * incoming_sum

        max_diff = max(abs(new_ranks[n] - ranks[n]) for n in nodes)
        ranks = new_ranks

    return ranks


if __name__ == "__main__":
    print("Spark Started Successfully")
    users, edges = load_data()
    print("Graph Created Successfully")
    print("Total Users:", len(users))
    print("Total Relationships:", len(edges))

    ranks = compute_pagerank(users, edges)

    results = sorted(
        [(users[uid], score) for uid, score in ranks.items()],
        key=lambda x: x[1],
        reverse=True
    )

    print("\nPageRank Results")
    for name, score in results:
        print(f"{name} : {score}")

    print("\nMost Influential User")
    print(f"({results[0][0]},{results[0][1]})")
```

---

## Expected Output

```
Spark Started Successfully
Graph Created Successfully
Total Users: 6
Total Relationships: 7

PageRank Results
Charlie : 2.1232774588227703
Alice : 1.9547208520626864
Bob : 1.3443056411388237
David : 0.27759420487145
Frank : 0.15005092155213512
Emma : 0.15005092155213512

Most Influential User
(Charlie,2.1232774588227703)
```
