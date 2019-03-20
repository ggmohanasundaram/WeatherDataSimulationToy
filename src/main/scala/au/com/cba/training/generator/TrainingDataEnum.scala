package au.com.cba.training.generator

import org.apache.spark.sql.{DataFrame, SparkSession}

object TrainingDataEnum extends Enumeration {

  type TrainingData = Value
  val Temperature, Humidity, Pressure, Condition, Unknown = Value

  def fromString(name: String): Value =
    values.find(_.toString.toLowerCase == name.toLowerCase()).getOrElse(Unknown)

  private[generator] def generateTemperature(inputTable: String)(implicit sparkSession: SparkSession): DataFrame = {
    val dataFrame = sparkSession.sql(
      s"""
         |select concat(temperature,' ','1:',latitude,' ','2:',longitude,' ','3:',elevation,' ','4:',day) from ${inputTable}
      """.stripMargin)
    dataFrame
  }

  private[generator] def generateHumidity(inputTable: String)(implicit sparkSession: SparkSession): DataFrame = {

    val dataFrame = sparkSession.sql(
      s"""
         |select concat(humidity,' ','1:',latitude,' ','2:',longitude,' ','3:',elevation,' ','4:',day) from ${inputTable}
      """.stripMargin)
    dataFrame

  }

  private[generator] def generatePressure(inputTable: String)(implicit sparkSession: SparkSession): DataFrame = {
    val dataFrame = sparkSession.sql(
      s"""
         |select concat(pressure,' ','1:',latitude,' ','2:',longitude,' ','3:',elevation,' ','4:',day) from ${inputTable}
      """.stripMargin)
    dataFrame
  }

  private[generator] def generateCondition(inputTable: String)(implicit sparkSession: SparkSession): DataFrame = {
    val dataFrame = sparkSession.sql(
      s"""
         |select concat(condition,' ','1:',latitude,' ','2:',longitude,' ','3:',elevation,' ','4:',day) from ${inputTable}
      """.stripMargin)

    dataFrame
  }

  def generateTrainingData(trainingData: TrainingData, inputTableName: String)(implicit sparkSession: SparkSession): DataFrame = trainingData match {
    case TrainingDataEnum.Temperature => generateTemperature(inputTableName)
    case TrainingDataEnum.Humidity => generateHumidity(inputTableName)
    case TrainingDataEnum.Pressure => generatePressure(inputTableName)
    case TrainingDataEnum.Condition => generateCondition(inputTableName)
  }

}
