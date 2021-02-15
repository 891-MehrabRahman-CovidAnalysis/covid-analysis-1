import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

object Main {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("us-age-spikes")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val path = setUpConnection(spark)
    val localPath = setUpConnection()
    val twitterDF = TwitterCovidAnalysis.readTwitterToDF(spark)
    val usDF = TwitterCovidAnalysis.readToDF(spark, localPath)
    
    // TwitterCovidAnalysis.groupByDate(usDF).show(335)
    // TwitterCovidAnalysis.ageGroupsInfectionCount(usDF).show()
    TwitterCovidAnalysis.twitterVolumeSpikes(twitterDF, usDF).show(335)
    
    spark.stop
  }

  /** Uses dataset from datalake directory instead of AWS
    * 
    * @return location of datalake directory
    */
  def setUpConnection(): String = {
    //      twitter-covid\twitter-covid-us\datalake\COVID-19_Cases_Summarized_by_Age_Group.csv
    return "datalake/COVID-19_Cases_Summarized_by_Age_Group.csv"
  }

  /** Set up AWS connection
    * Must have AWS Access Key ID and AWS Secret Access Key
    * @param spark
    * @return path - aws connection path as a string
    */
  def setUpConnection(spark: SparkSession): String = {
    var path = ""
    if (
      // Check environment variables
      !sys.env.contains("AWS_ACCESS_KEY_ID") || !sys.env.contains(
        "AWS_SECRET_ACCESS_KEY"
      )
    ) {
      System.err.println(
        "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY expected but not found"
      )
      spark.close()
      System.exit(1)
    } else {
      // AWS Path
      path =
        "s3a://covid-analysis-p3/datalake/twitter-covid/COVID-19_Cases_Summarized_by_Age_Group.csv"
      // Load in AWS credientials from environment
      spark.sparkContext.hadoopConfiguration
        .set("fs.s3a.awsAccessKeyId", sys.env("AWS_ACCESS_KEY_ID"))
      spark.sparkContext.hadoopConfiguration
        .set("fs.s3a.awsSecretAccessKey", sys.env("AWS_SECRET_ACCESS_KEY"))
      spark.sparkContext.hadoopConfiguration
        .set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    }
    return path
  }
}
