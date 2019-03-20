package au.com.cba.weatherdata.generator

import java.text.SimpleDateFormat
import java.util.Date

import au.com.cba.common.domain.{Condition, Locations, WeatherDataEnum, WeatherReport}
import au.com.cba.common.generator.DataGenerator
import au.com.cba.common.util.{SourceConfig, SparkIo}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

class WeatherDataGenerator(override val sourceConfig: SourceConfig, override val sparkIo: SparkIo,
                           override val inputName: Option[String]=None, override val outputName: Option[String]=None, val count: Int = 10)
                          (implicit sparkSession: SparkSession) extends DataGenerator {


  private val inputPath: String =  s"${System.getProperty("user.dir")}/trainingData/"

  private def initiateModelObjects(className: String): TrainingModelBuilder = {
    val trainingModel = Class.forName(s"au.com.cba.weatherdata.generator.${className}Model")
    trainingModel.getConstructor(classOf[String], classOf[SparkSession]).
      newInstance(s"${inputPath}${className.toLowerCase}.txt", sparkSession).asInstanceOf[TrainingModelBuilder]
  }


  lazy val models = WeatherDataEnum.values.map(x => initiateModelObjects(x.toString))


  override def extractData()(implicit sparkSession: SparkSession): Unit = {

    models.map(x => x.data)
  }

  override def processData()(implicit sparkSession: SparkSession): Unit = {
    models.map(x => x.trainingModelBuilder)
  }

  override def loadData()(implicit sparkSession: SparkSession): Unit = {

    val random = new scala.util.Random
    val noise = random.nextInt((1 - (-1)) + 1)

    val dayOfWeek = random.nextInt((7 - 1) + 1)
    val dateTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date())

    (0 to count - 1).map(x => {
      val position = Locations.locations(x)
      val newPoint = Vectors.dense(position.latitude, position.longitude, position.elevation, dayOfWeek)
      val prediction = models.map(x => (x.builderName -> x.trainingModelBuilder.predict(newPoint))).toMap
      println(WeatherReport(x, position, dateTime, Condition.apply(prediction("Condition").toInt).toString,
        prediction("Temperature"), prediction("Pressure"), prediction("Humidity")))

    })
  }
}
