import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object GraphXPageRank {
  def main(args: Array[String]): Unit = {
    // Start Spark Session
    val spark = SparkSession.builder()
      .appName("GraphX PageRank")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    println("Spark Started Successfully")

    // Read users
    val users: RDD[(VertexId, String)] =
      sc.textFile("data/vertices.csv")
        .filter(!_.startsWith("id,"))
        .map { line =>
          val fields = line.split(",")
          (fields(0).toLong, fields(1))
        }

    // Read relationships
    val relationships: RDD[Edge[Int]] =
      sc.textFile("data/edges.csv")
        .filter(!_.startsWith("src,"))
        .map { line =>
          val fields = line.split(",")
          Edge(fields(0).toLong, fields(1).toLong, 1)
        }

    // Create GraphX graph
    val graph = Graph(users, relationships)
    println("Graph Created Successfully")
    println("Total Users: " + graph.numVertices)
    println("Total Relationships: " + graph.numEdges)

    // Apply PageRank with convergence tolerance 0.0001
    val ranks = graph.pageRank(0.0001).vertices

    // Combine names and PageRank scores
    val result = users.join(ranks)
      .map {
        case (id, (name, rank)) =>
          (name, rank)
      }
      .sortBy(_._2, ascending = false)

    // Display results
    println("\nPageRank Results")
    result.collect().foreach {
      case (name, rank) =>
        println(name + " : " + rank)
    }

    // Find most influential user
    println("\nMost Influential User")
    println(result.first())

    // Stop Spark
    spark.stop()
  }
}
