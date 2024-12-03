import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.functions._

object SensorHumidityProcessor {
  // Declare summary statistics variables globally
  var totalRecords: Long = 0
  var validCount: Long = 0
  var invalidCount: Long = 0
  var totalFilesProcessed: Long = 0
  var sortedResult: org.apache.spark.sql.DataFrame = _

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.exit(1)
    }

    val inputPath = args(0)
    println(s"Processing data from folder: $inputPath")

    val spark = SparkSession.builder()
      .appName("SensorHumidityProcessor")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try {
      val rawData = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(s"$inputPath/*.csv")
        
      totalFilesProcessed = rawData.inputFiles.length
      totalRecords = rawData.count()

      // Check if required columns exist
      if (!rawData.columns.contains("humidity") || !rawData.columns.contains("sensor-id")) {
        println("Error: Missing required columns (humidity or sensor-id) in the input data.")
        System.exit(1)
      }

      // Filter valid and invalid data
      val validData = rawData.filter(col("humidity").cast("double").isNotNull && !col("humidity").isNaN)
      val invalidData = rawData.filter(col("humidity").cast("double").isNull || col("humidity").isNaN)

      validCount = validData.count()
      invalidCount = invalidData.count()

      // Aggregate for sensors with valid data (ignoring NaN)
      val validAggregates = validData.groupBy("sensor-id")
        .agg(
          min(col("humidity").cast("double")).alias("min"),
          avg(col("humidity").cast("double")).alias("avg"),
          max(col("humidity").cast("double")).alias("max")
        )
        .na.fill(Double.NaN, Seq("min", "avg", "max"))

      // Aggregate for sensors with only NaN or null values
      val sensorsWithNaN = rawData.groupBy("sensor-id").agg(
        count(when(col("humidity").isNaN || col("humidity").isNull, 1)).alias("nan_count"),
        count("*").alias("total_count")
      ).filter(col("nan_count") === col("total_count"))

      val NaNData = sensorsWithNaN
        .withColumn("min", lit(Double.NaN))
        .withColumn("avg", lit(Double.NaN))
        .withColumn("max", lit(Double.NaN))

      // Union the valid aggregates and NaN data
      val result = validAggregates.union(NaNData.select("sensor-id", "min", "avg", "max"))
      sortedResult = result.withColumn("sortOrder", 
        when(col("min").isNaN && col("avg").isNaN && col("max").isNaN, 1).otherwise(0))
        .orderBy(col("sortOrder"), desc("avg"))

    } catch {
      case e: Exception =>
        println("Error processing the data")
    } finally {
      // Print the summary statistics in the finally block
      println(s"Total number of records processed: $totalRecords")
      println(s"Num of valid measurements: $validCount")
      println(s"Num of failed measurements (invalid or NaN): $invalidCount")
      println(s"Total files processed: $totalFilesProcessed")

      // Print the result: Sensors with the highest avg humidity (NaN last)
      println("Sensors with highest avg humidity:")
      sortedResult.select("sensor-id", "min", "avg", "max").show()

      // Stop Spark session
      spark.stop()
    }
  }
}
