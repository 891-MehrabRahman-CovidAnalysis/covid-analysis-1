package response

import org.apache.log4j.{Level, Logger}
import utilites.{DataFrameBuilder, s3DAO}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{max, sum}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/** Question: Which Regions handled COVID-19 the best, assuming our metrics are change in GDP by percentage and COVID-19
  * infection rate per capita?
  *
  * Answers the following queries using spark dataframes and outputs to console:
  * "Average New Cases per Day in Each Region"
  * "Average New Cases per Million People per Day in Each Region (normalized before region grouping)"
  * "Average New Cases per Million People per Day in Each Region"
  * "Total Cases in Each Region"
  * "Total Cases per Million People in Each Region (normalized before region grouping)"
  * "Total Cases per Million People in Each Region"
  * "Average GDP Percent Change in Each Region"
  * "Average GDP per Capita Percent Change in Each Region"
  */
object CovidResponse {

  /** Main
    * Creates the needed dataframe with DataFrameBuilder
    * Outputs the data as formatted tables in the console as well as outputs the formatted data to dataframes in Spark.
    * The DataFrameBuilder prepares raw data from two source files by selecting various, needed columns and renaming
    * a few. The two tables are then joined with the joinCaseEcon method. The resulting dataframe is passed in this
    * methods signature.  The RankRegions class uses methods to group and sort by some metric passed as an argument.
    * This is used just prior to displaying results.
    */
  def main(args: Array[String]): Unit = {
    // Set logging level.
    Logger.getLogger("org").setLevel(Level.WARN)

    //Class dependencies and app config.
    val s3 = s3DAO()
    val dfb = new DataFrameBuilder
    s3.setLocalLakePath("CovidResponse/src/main/resources/datalake")
    s3.setLocalWarehousePath("CovidResponse/src/main/resources/datawarehouse")
    val fileNames = Map(
      "covidSrc" -> "owid-covid-data.csv",
      "regionSrc" -> "region_dictionary.json",
      "econSrc" -> "economic_data_2018-2021.tsv"
    )

    //Spark setup.
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    //Build DF.
    val data = dfb.build(spark, fileNames, s3)

    //format data for queries.
    data
      .withColumn("population", $"total_cases" / $"total_cases_per_million")
      .groupBy("region", "country")
      .agg(max($"population") as "population")
      .groupBy("region")
      .agg(sum($"population") as "population")
      .cache()

    //Show all results.
    println("\nAverage New Cases per Day in Each Region")
    val newRegionalCases =
      RankRegions.rankByMetricLow(spark, data, "new_cases").cache()
    s3.localSaveAndUploadTos3(newRegionalCases, "new_regional_cases")

    newRegionalCases.show()

    println(
      "\nAverage New Cases per Million People per Day in Each Region (normalized before region grouping)")
    val newRegionalCasesPerMil =
      RankRegions.rankByMetricLow(spark, data, "new_cases_per_million").cache()
    s3.localSaveAndUploadTos3(newRegionalCasesPerMil,
                              "new_regional_cases_per_million")

    newRegionalCasesPerMil.show()

    println("\nAverage New Cases per Million People per Day in Each Region")
    val newCasesDailyPerMil =
      RankRegions.rankByMetricLow(spark, data, "new_cases", "pop").cache()
    s3.localSaveAndUploadTos3(newCasesDailyPerMil, "new_cases_daily_per_mil")

    println("\nTotal Cases in Each Region")
    val totalRegionalCases =
      RankRegions.rankByMetricLow(spark, data, "total_cases", "max").cache()
    s3.localSaveAndUploadTos3(totalRegionalCases, "total_regional_cases")

    println(
      "\nTotal Cases per Million People in Each Region (normalized before region grouping)")
    RankRegions
      .rankByMetricLow(spark, data, "total_cases_per_million", "max")
      .show()

    println("\nTotal Cases per Million People in Each Region")
    val totalRegionalCasesPerMil =
      RankRegions.rankByMetricLow(spark, data, "total_cases", "maxpop").cache()
    s3.localSaveAndUploadTos3(totalRegionalCasesPerMil,
                              "total_regional_cases_per_mil")

    totalRegionalCasesPerMil.show()

    println("\nAverage GDP Percent Change in Each Region")
    val deltaGDP = RankRegions
      .changeGDP(spark, data, "gdp_currentPrices_usd", percapita = false)
      .cache()
    s3.localSaveAndUploadTos3(deltaGDP, "regional_change_gdp")

    deltaGDP.show()

    println("\nAverage GDP per Capita Percent Change in Each Region")
    val deltaGDPerCapita = RankRegions
      .changeGDP(spark, data, "gdp_perCap_currentPrices_usd", percapita = false)
      .cache()

    println(
      s"Saving all results to s3 bucket: ${s3.BUCKET_NAME} \nand local directory ${s3.getLocalLakePath}...")
    //wraps last result in future to await its completion.
    val saveLastResult: Future[Unit] = Future {
      s3.localSaveAndUploadTos3(deltaGDPerCapita,
                                "regional_change_gdp_percapita")
    }
    saveLastResult.onComplete(_ => {
      println("Complete!")
      spark.stop()
      System.exit(0)
    })
    deltaGDP.show()
  }

}
