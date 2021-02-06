package HashtagCountComparison

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalamock.scalatest.MockFactory
import org.apache.spark.sql.{SparkSession,Row}


class RunnerSpec extends AnyFlatSpec with Matchers{
  
  //create spark session and test DataFrame for the test suite
  val testSpark = SparkSession.builder().master("local").appName("Tester").getOrCreate()
  import testSpark.implicits._
  val testDF = testSpark.read.format("text").load("test-data.txt")
  val finalData = Array[Row](Row("Covid",1),(Row("Non-Covid",0)))

  
  //Tests for getInputPath()
    "getInputPath case 0" should "return the path to s3" in {
    Runner.getInputPath(0) shouldEqual "s3://covid-analysis-p3/datalake/twitter-general/dec_11-dec_25/"
  }

  "getInputPath case 1" should "return the path to s3" in {
    Runner.getInputPath(1) shouldEqual "s3://covid-analysis-p3/datalake/twitter-general/dec_26-jan_05/"
  }

  "getInputPath case 2" should "return the path to s3" in {
    Runner.getInputPath(2) shouldEqual "s3://covid-analysis-p3/twitter-general/data-lake/Jan_06-Pres/"
  }

  //tests for readToDF()
  "readToDF case 0" should "return a DataFrame containing the input from path" in {
    val compDF = Runner.readToDF(testSpark,"test-data.txt")

    assert(compDF.collect().equals(testDF.collect()))
  }

  //tests for manipulateDataFrame
  "manipulateDataFrame case 0" should "return a dataframe with the expected values" in {
    assert(Runner.manipulateDataFrame(testDF).collect().equals(finalData))
  }


  //tests for ouput


}