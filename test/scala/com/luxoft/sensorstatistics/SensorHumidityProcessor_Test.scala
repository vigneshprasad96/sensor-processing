import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

class SensorHumidityProcessorTest extends AnyFunSuite with BeforeAndAfterAll {
  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("SensorHumidityProcessorTest")
      .master("local[*]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  // Sample Data
  def getInputData: DataFrame = {
    import spark.implicits._
    val data = List(
      List("sensor1", "78"),
      List("sensor2", "NaN"),
      List("sensor3", "45"),
      List("sensor3", "null"),
      List("sensor4", "91")
    )
    data.map(row => (row(0), row(1))).toDF("sensor-id", "humidity")
  }

  // Processing logic
  def processValidData(inputData: DataFrame): DataFrame = {
    import spark.implicits._
    inputData.filter($"humidity".cast("double").isNotNull && !$"humidity".isNaN)
      .groupBy("sensor-id")
      .agg(
        min($"humidity".cast("double")).alias("min"),
        avg($"humidity".cast("double")).alias("avg"),
        max($"humidity".cast("double")).alias("max")
      )
  }

  def processInvalidData(inputData: DataFrame): DataFrame = {
    import spark.implicits._
    inputData.filter($"humidity".cast("double").isNull || $"humidity".isNaN)
      .select("sensor-id")
      .distinct()
      .withColumn("min", lit(Double.NaN))
      .withColumn("avg", lit(Double.NaN))
      .withColumn("max", lit(Double.NaN))
  }

  // Test Case 1: Validate Processing of Valid Data
  test("Test processing of valid data") {
    val inputData = getInputData
    val validData = processValidData(inputData)

    import spark.implicits._
    val expectedValid = List(
      List("sensor1", 78.0, 78.0, 78.0),
      List("sensor3", 45.0, 45.0, 45.0),
      List("sensor4", 91.0, 91.0, 91.0)
    ).map(row => (row(0), row(1), row(2), row(3))).toDF("sensor-id", "min", "avg", "max")

    assert(validData.except(expectedValid).isEmpty, "Valid data processing failed")
  }

  // Test Case 2: Validate Processing of NaN/Invalid Data
  test("Test processing of NaN/invalid data") {
    val inputData = getInputData
    val invalidData = processInvalidData(inputData)

    import spark.implicits._
    val expectedNaN = List(
      List("sensor2", Double.NaN, Double.NaN, Double.NaN)
    ).map(row => (row(0), row(1), row(2), row(3))).toDF("sensor-id", "min", "avg", "max")

    assert(invalidData.except(expectedNaN).isEmpty, "Invalid data processing failed")
  }

  // Test Case 3: Validate Combined Results
  test("Test combined result for valid and invalid data") {
    val inputData = getInputData
    val validData = processValidData(inputData)
    val invalidData = processInvalidData(inputData)
    val combinedResult = validData.union(invalidData)

    import spark.implicits._
    val expectedCombined = List(
      List("sensor1", 78.0, 78.0, 78.0),
      List("sensor3", 45.0, 45.0, 45.0),
      List("sensor4", 91.0, 91.0, 91.0),
      List("sensor2", Double.NaN, Double.NaN, Double.NaN)
    ).map(row => (row(0), row(1), row(2), row(3))).toDF("sensor-id", "min", "avg", "max")

    assert(combinedResult.except(expectedCombined).isEmpty, "Combined result validation failed")
  }
}
