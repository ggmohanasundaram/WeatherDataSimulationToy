package au.com.cba.common.util

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

class SparkIo {

  private val logger = LoggerFactory.getLogger(classOf[SparkIo])


  def loadJsonFromString(jsonInput: String)(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._
    sparkSession.sqlContext.read.option("multiline", "true").json(Seq(jsonInput).toDS())
  }


  def loadJsonFromPath(path: String)(implicit sparkSession: SparkSession) = {
    sparkSession.sqlContext.read.option("multiline", "true").json(path)
  }

  def writeJson[T](dataset: Dataset[T], filePath: String)(implicit sparkSession: SparkSession) = {
    dataset.write.mode(SaveMode.Overwrite).json(filePath)
  }

  def writeTextFile[T](dataset: Dataset[T], filePath: String)(implicit sparkSession: SparkSession) = {
    val repartion = dataset.repartition(1)
    repartion.write.mode(SaveMode.Overwrite).option("header", false).text(filePath)
  }
  def writeOrc[T](dataset: Dataset[T], filePath: String)(implicit sparkSession: SparkSession) = {
    val repartion = dataset.repartition(1)
    repartion.write.mode(SaveMode.Overwrite).option("header", false).orc(filePath)
  }

  def readOrc(filePath:String)(implicit sparkSession: SparkSession) ={
    sparkSession.read.orc(filePath)
  }
}
