import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TwitterCovidAnalysis {

  /** Simple function to read US COVID data from s3 bucket.
    *
    * @param spark
    * @param path
    */
  def readToDF(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)
      .cache
  }

  /** Simple function to read Twitter COVID data from s3 bucket.
    * 
    * @param spark
    * 
    */
  def readTwitterToDF(spark: SparkSession): DataFrame = {
    //Took 35 mins to execute
    // val path = "s3a://covid-analysis-p3/datalake/twitter-covid/full_dataset_clean.tsv"
    val path = "s3a://covid-analysis-p3/datalake/twitter-covid/twitter-1000.tsv"

    spark.read
      .option("sep", "\t")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(
        path
      )
      .cache()
  }

  /** Groups dataframe by day.
    * Returns DF with date(yyyy/mm/dd) and infection counts on that date
    * @param df
    */
  def groupByDate(df: DataFrame): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    df.select("Specimen Collection Date", "New Confirmed Cases")
      .groupBy("Specimen Collection Date")
      .sum("New Confirmed Cases")
      .orderBy($"Specimen Collection Date".asc)

  }

  /** Groups dataframe by age groups.
    * Returns DF of age groups and infection counts.
    *
    * @param df
    */
  def ageGroupsInfectionCount(df: DataFrame): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    df.select("Age Group", "New Confirmed Cases")
      .groupBy("Age Group")
      .sum("New Confirmed Cases")
      .orderBy($"Age Group".asc)
  }

  /** Groups by day with highest spike.
    * Returned columns: Date, infection rate (age 5-30), and Twitter Volume.
    * @param df
    */
  def twitterVolumeSpikes(twitterDF: DataFrame, usDF: DataFrame): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    // date, count columns from twitterDF
    val tDF = twitterDF
      .select("date")
      .groupBy("date")
      .count()
      .orderBy($"date".asc)
    val twitter = tDF.withColumn("date", to_date($"date"))
    // date, infection sums from usDF
    val us = groupByDate(usDF)
      .withColumn("Specimen Collection Date", to_date($"Specimen Collection Date", "yyyy/MM/dd"))

    // Join results
    val result = us
      .join(twitter, us("Specimen Collection Date") === twitter("date"), "inner")
      .select("Specimen Collection Date", "sum(New Confirmed Cases)", "count")
    result
      .withColumnRenamed("count", "Twitter Volume")
      .withColumnRenamed("sum(New Confirmed Cases)", "New Confirmed Cases")
      .orderBy($"Specimen Collection Date".asc)
    
  }
}
