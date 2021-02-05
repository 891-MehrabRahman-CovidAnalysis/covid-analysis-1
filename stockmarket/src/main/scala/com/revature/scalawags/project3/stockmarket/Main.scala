import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object Main {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("Covid Composite Index")
      .master("local[4]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    fileReader(spark)
    spark.stop()

    def fileReader(spark: SparkSession) {
      import spark.implicits._
      val CSVfile = spark.
                        read.option("inferScheme", "true").
                        csv("C:/Users/bryan/CovidAnalysis/covid-analysis-1/data/datalake/Africa_Egypt_CompositeIndex.csv")
      
      println("printing schema")
      CSVfile.printSchema()
      
      //Displays table ordered by date from 12/31/2020 - 01/01/2020
      CSVfile.orderBy($"_c0".asc).show()

      //Append table back to same file? Double check negative and positive values
      val windowSpec = Window.orderBy($"_c0".desc) 
      CSVfile.withColumn("Daily_Comp_Diff", $"_c4" - when((lag("_c4", 1).over(windowSpec)).isNull, 0).otherwise(lag("_c4", 1).over(windowSpec)))
      .show(false)

    }
  }
}
     