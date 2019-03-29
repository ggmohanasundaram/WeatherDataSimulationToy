package au.com.cba.practice

import au.com.cba.common.util.{SparkIo, WeatherDataSimulatorUtil}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.FeatureHasher
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql.DataFrame

object WeatherData {

  val classifier = new RandomForestClassifier()
  val regression = new RandomForestRegressor()

  val classifier_pipeline = new Pipeline()
    .setStages(Array(classifier))

  val regression_pipeline = new Pipeline()
    .setStages(Array(regression))

  def main(args: Array[String]): Unit = {

    implicit val sparkSession = WeatherDataSimulatorUtil.createSparkSession("WeatherData")
    import sparkSession.implicits._
    try {
      val sparkIo = new SparkIo
      /* val generator = new TrainingDataGenerator(ApplicationConfig.init("/conf/trainingdata.conf").getSourceConfig().get, sparkIo)
       generator.extractData()
       generator.processData()
       generator.loadData()
       val outPut = sparkSession.sql(
         """
           |select * from TrainingData
         """.stripMargin)

       sparkIo.writeOrc(outPut,"/home/gjnmmq/AMP/output/testdata/")*/

      val trainingData = sparkIo.readOrc("/home/gjnmmq/AMP/output/testdata/")

      val condition = trainingData.select("latitude", "longitude", "elevation", "day", "condition").withColumnRenamed("condition", "label")
      val temperature = trainingData.select("latitude", "longitude", "elevation", "day", "temperature").withColumnRenamed("temperature", "label")
      val pressure = trainingData.select("latitude", "longitude", "elevation", "day", "pressure").withColumnRenamed("pressure", "label")
      val humidity = trainingData.select("latitude", "longitude", "elevation", "day", "humidity").withColumnRenamed("humidity", "label")

      val condition_vector = new FeatureHasher().setInputCols("latitude", "longitude", "elevation", "day", "label")
        .setOutputCol("features").transform(condition)

      val temperature_vector = new FeatureHasher().setInputCols("latitude", "longitude", "elevation", "day", "label")
        .setOutputCol("features").transform(temperature)

      val pressure_vector = new FeatureHasher().setInputCols("latitude", "longitude", "elevation", "day", "label")
        .setOutputCol("features").transform(pressure)

      val humidity_vector = new FeatureHasher().setInputCols("latitude", "longitude", "elevation", "day", "label")
        .setOutputCol("features").transform(humidity)


      val conditionModel = getClasifierModer(condition_vector)
      val tempreature_model = getRegressionModel(temperature_vector)
      val pressure_model = getRegressionModel(pressure_vector)
      val humidity_model = getRegressionModel(humidity_vector)

      val testData = Seq((-40.98, 145.72, 58, 7), (-33.80, 150.98, 41, 5), (-33.80, 150.98, 41, 3)).toDF("latitude", "longitude", "elevation", "day")

      val vector_testData = new FeatureHasher().setInputCols("latitude", "longitude", "elevation", "day")
        .setOutputCol("features").transform(testData)

      val newCondition = conditionModel.transform(vector_testData)
      val newTempreature = tempreature_model.transform(vector_testData)
      val newPreasure = pressure_model.transform(vector_testData)
      val newHumidity = humidity_model.transform(vector_testData)

      newCondition.show()
      newTempreature.show()
      newPreasure.show()
      newHumidity.show()

    }
    catch {
      case e => throw e
    }
    finally {
      sparkSession.stop()
    }
  }

  def getClasifierModer(data: DataFrame) = {
    classifier_pipeline.fit(data)
  }

  def getRegressionModel(data: DataFrame) = {
    regression_pipeline.fit(data)
  }

}
