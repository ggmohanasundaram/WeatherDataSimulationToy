package au.com.cba.weatherdata.generator

import au.com.cba.common.domain.WeatherDataEnum
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession

trait TrainingModelBuilder {

  val builderName:String

  val inputPath: String

  implicit val sparkSession: SparkSession

  val trainingModel = new TrainingModel()

  val data = readTrainingData()

  def readTrainingData()(implicit sparkSession: SparkSession) = MLUtils.loadLibSVMFile(sparkSession.sparkContext, inputPath)

  val trainingModelBuilder: RandomForestModel

}

class TemperatureModel(override val inputPath: String)(override implicit val sparkSession: SparkSession) extends TrainingModelBuilder {

  override val builderName:String = WeatherDataEnum.Temperature.toString

  override val trainingModelBuilder = {
    trainingModel.trainRegressor(data)
  }
}

class HumidityModel(override val inputPath: String)(override implicit val sparkSession: SparkSession) extends TrainingModelBuilder {

  override val builderName:String = WeatherDataEnum.Humidity.toString

  override val trainingModelBuilder = {
    trainingModel.trainRegressor(data)
  }

}

class PressureModel(override val inputPath: String)(override implicit val sparkSession: SparkSession) extends TrainingModelBuilder {

  override val builderName:String = WeatherDataEnum.Pressure.toString

  override val trainingModelBuilder = {
    trainingModel.trainRegressor(data)
  }
}

class ConditionModel(override val inputPath: String)(override implicit val sparkSession: SparkSession) extends TrainingModelBuilder {

  override val builderName:String = WeatherDataEnum.Condition.toString

  override val trainingModelBuilder = {
    trainingModel.trainClassifier(data)
  }
}
