import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Day23DStreamsBasics {

  def main(args: Array[String]): Unit = {

    println("\n==========================================")
    println("      DAY 23 - DSTREAMS BASICS")
    println("==========================================")

    // ---------------------------------------------------------
    // Spark Configuration
    // ---------------------------------------------------------
    val conf = new SparkConf()
      .setAppName("Day23DStreamsBasics")
      .setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.ui.showConsoleProgress", "false")

    // ---------------------------------------------------------
    // Create Streaming Context
    // Batch interval = 5 seconds
    // ---------------------------------------------------------
    val ssc = new StreamingContext(conf, Seconds(5))

    // Hide Spark INFO/WARN messages
    ssc.sparkContext.setLogLevel("ERROR")

    // ---------------------------------------------------------
    // Read real-time data from TCP socket
    // ---------------------------------------------------------
    val lines = ssc.socketTextStream("localhost", 9999)

    // ---------------------------------------------------------
    // MAP
    // Convert log messages to uppercase
    // ---------------------------------------------------------
    val upperCaseLogs = lines.map(_.toUpperCase)

    upperCaseLogs.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println("\n========== MAP ==========")
        rdd.collect().foreach(println)
      }
    }

    // ---------------------------------------------------------
    // FILTER
    // Select only ERROR messages
    // ---------------------------------------------------------
    val errorLogs = lines.filter(_.contains("ERROR"))

    errorLogs.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println("\n========== FILTER - ERROR LOGS ==========")
        rdd.collect().foreach(println)
      }
    }

    // ---------------------------------------------------------
    // FLATMAP
    // Split log messages into words
    // ---------------------------------------------------------
    val words = lines.flatMap(_.split("\\s+"))

    words.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println("\n========== FLATMAP - WORDS ==========")
        rdd.collect().foreach(println)
      }
    }

    // ---------------------------------------------------------
    // ERROR COUNT
    // Count ERROR messages in each 5-second batch
    // ---------------------------------------------------------
    val errorCount = errorLogs
      .map(_ => 1)
      .reduce(_ + _)

    errorCount.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        val count = rdd.collect().head

        println("\n========== ERROR COUNT ==========")
        println(s"ERROR messages in this batch: $count")
        println("=================================\n")
      }
    }

    // ---------------------------------------------------------
    // Start Streaming
    // ---------------------------------------------------------
    ssc.start()

    println("\nStreaming application started")
    println("Batch interval : 5 seconds")
    println("Socket         : localhost:9999")
    println("Waiting for log messages...")
    println("Send messages using nc in another terminal.\n")

    // ---------------------------------------------------------
    // Keep application running
    // ---------------------------------------------------------
    ssc.awaitTermination()
  }
}
