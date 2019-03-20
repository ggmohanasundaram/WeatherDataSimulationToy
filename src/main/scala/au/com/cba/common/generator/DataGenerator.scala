package au.com.cba.common.generator

import au.com.cba.common.util.{SourceConfig, SparkIo}
import org.apache.spark.sql.SparkSession

trait DataGenerator {

  val sourceConfig: SourceConfig

  val sparkIo: SparkIo

  val inputName: Option[String]

  val outputName: Option[String]

  def extractData()(implicit sparkSession: SparkSession)

  def processData()(implicit sparkSession: SparkSession)

  def loadData()(implicit sparkSession: SparkSession)

}
