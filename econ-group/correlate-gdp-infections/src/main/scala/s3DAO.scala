package econ

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.AmazonClientException
import com.amazonaws.AmazonServiceException
import java.io.File
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.FileOutputStream
import org.apache.commons.io.IOUtils

case class s3DAO(amazonS3Client: AmazonS3Client, var BUCKET_NAME: String) {

  def createNewBucket(bucketName: String): Unit = {
    try {
      amazonS3Client.createBucket(bucketName)
      this.BUCKET_NAME = bucketName
    } catch {
      case e: AmazonClientException => System.err.println("Exception: " + e.toString)
    }
  }

  def uploadFile(file: File, FILE_NAME: String): Unit = {
    try {
      amazonS3Client.putObject(BUCKET_NAME, FILE_NAME, file)
    } catch {
      case e: AmazonClientException => System.err.println("Exception: " + e.toString)
    }
  }

  // download file and console out each line
  def downloadFile(fileName: String): Unit = {
    try {
      val obj = amazonS3Client.getObject(BUCKET_NAME, fileName)
      val reader = new BufferedReader(new InputStreamReader(obj.getObjectContent))
      var line = reader.readLine
      while (line != null) {
        println(line)
        line = reader.readLine
      }
    } catch {
      case e: AmazonClientException => System.err.println("Exception: " + e.toString)
    }
  }

  // download file and write to local file system
  def copyFileFromBucket(fileName: String): Unit = {
    try {
      val obj = amazonS3Client.getObject(BUCKET_NAME, fileName)
      val bytes = IOUtils.toByteArray(obj.getObjectContent)
      val file = new FileOutputStream("file-path/" + fileName)
      file.write(bytes)
    } catch {
      case e: AmazonClientException => System.err.println("Exception: " + e.toString)
    }
  }
}

object s3DAO {
  def apply(): s3DAO = {
    val BUCKET_NAME = "###"
    val FILE_PATH = "" //FIXME: path of the tsv/csv that contains the daily case or econ stats
    val FILE_NAME = "###"
    val AWS_ACCESS_KEY = System.getenv("AWS_ACCESS_KEY_ID")
    val AWS_SECRET_KEY = System.getenv("AWS_SECRET_ACCESS_KEY")
    var (awsCredentials, amazonS3Client) = (BasicAWSCredentials, AmazonS3Client)

    try {
      awsCredentials = new BasicAWSCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY)
      amazonS3Client = new AmazonS3Client(awsCredentials)
    } catch {
      case ase: AmazonServiceException => System.err.println("Exception: " + ase.toString)
      case ace: AmazonClientException => System.err.println("Exception: " + ace.toString)
    }
    new s3DAO(amazonS3Client, BUCKET_NAME)
  }
}