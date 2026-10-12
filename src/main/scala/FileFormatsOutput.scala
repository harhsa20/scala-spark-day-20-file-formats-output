
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FileFormatsOutput {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 20 - File Formats and Output")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val outputPath = "output"

    // Sample daily sales data
    val salesDF = Seq(
      (1, "Laptop", 50000.0, "2026-10-10"),
      (2, "Phone", 20000.0, "2026-10-10"),
      (3, "Tablet", 15000.0, "2026-10-11"),
      (4, "Laptop", 55000.0, "2026-10-11"),
      (5, "Phone", 22000.0, "2026-11-01"),
      (6, "Tablet", 18000.0, "2026-11-01")
    ).toDF("sale_id", "product", "amount", "sale_date")
      .withColumn("sale_date", to_date(col("sale_date")))
      .withColumn("year", year(col("sale_date")))
      .withColumn("month", month(col("sale_date")))
      .withColumn("day", dayofmonth(col("sale_date")))

    println("\n========== ORIGINAL SALES DATA ==========")
    salesDF.show(false)

    // Write CSV
    salesDF.write
      .mode("overwrite")
      .option("header", "true")
      .csv(s"$outputPath/csv")

    // Write JSON
    salesDF.write
      .mode("overwrite")
      .json(s"$outputPath/json")

    // Write Parquet
    salesDF.write
      .mode("overwrite")
      .parquet(s"$outputPath/parquet")

    println("\n========== READ CSV ==========")
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(s"$outputPath/csv")
      .show(false)

    println("\n========== READ JSON ==========")
    spark.read.json(s"$outputPath/json").show(false)

    println("\n========== READ PARQUET ==========")
    spark.read.parquet(s"$outputPath/parquet").show(false)

    // Repartition before writing
    println("\n========== REPARTITIONED PARQUET ==========")

    salesDF
      .repartition(3)
      .write
      .mode("overwrite")
      .parquet(s"$outputPath/repartitioned_parquet")

    // Partition by year, month, and day
    println("\n========== PARTITIONED SALES OUTPUT ==========")

    salesDF
      .repartition(col("year"), col("month"), col("day"))
      .write
      .mode("overwrite")
      .partitionBy("year", "month", "day")
      .parquet(s"$outputPath/daily_sales")

    println("CSV, JSON, Parquet, repartitioned output, and daily partitioned output created.")
    println(s"Input row count: ${salesDF.count()}")

    spark.stop()
  }
}
