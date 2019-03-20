package au.com.cba.training.generator

import au.com.cba.common.util.{ApplicationConfig, SparkIo}
import au.com.cba.testutil.TestUtils
import org.specs2.mock.Mockito

class TrainingDataGeneratorTest extends TestUtils with Mockito {


  private val expected = "location,latitude,longitude,elevation,condition,temperature,humidity,pressure,day"
  private val data = Seq(("some_data", "some_latitude", "some_longitude", "some_elevation", "some_condition",
    "some_temperature", "some_humidity", "some_pressure", "some_day"),
    ("some_data", "some_latitude", "some_longitude", "some_elevation", "some_condition",
      "some_temperature", "some_humidity", "some_pressure", "some_day"))

  "Method extractData in TrainingDataGenerator" should {
    "write empty dataframe for no responses" in {
      val testData: String = scala.io.Source.fromURL(getClass.getResource("/testData/weatherData.json")).getLines.mkString
      val appConfig = ApplicationConfig.init("/conf/test.conf").getSourceConfig().get
      val generator = spy(new TrainingDataGenerator(appConfig, new SparkIo()))
      doReturn((100, Right(testData))).when(generator).getWeatherData(anyString)
      generator.extractData()
      val output = sparkSession.sql("select * from TrainingData")
      output.count() mustEqual (0)
    }
    "write  data frame for  responses" in {
      val testData: String = scala.io.Source.fromURL(getClass.getResource("/testData/weatherData.json")).getLines.mkString
      val appConfig = ApplicationConfig.init("/conf/test.conf").getSourceConfig().get
      val generator = spy(new TrainingDataGenerator(appConfig, new SparkIo()))
      val expected = "location,elevation,latitude,longitude,time,precipType,temperature,humidity,pressure"
      doReturn((200, Right(testData))).when(generator).getWeatherData(anyString)
      generator.extractData()
      val output = sparkSession.sql("select * from TrainingData")
      output.count() mustEqual (30)
      output.schema.toList.map(x => x.name).mkString(",") mustEqual expected
    }
  }

  "Method Processdata in TrainingDataGenerator" should {
    "Throw exception when there was no table" in {
      val testData: String = scala.io.Source.fromURL(getClass.getResource("/testData/weatherData.json")).getLines.mkString
      val appConfig = ApplicationConfig.init("/conf/test.conf").getSourceConfig().get
      val generator = spy(new TrainingDataGenerator(appConfig, new SparkIo()))
      doReturn((100, Right(testData))).when(generator).getWeatherData(anyString)
      generator.extractData()
      generator.processData() must throwA[Exception]
    }
    "Process and dataframe and decorate them" in {
      val testData: String = scala.io.Source.fromURL(getClass.getResource("/testData/weatherData.json")).getLines.mkString
      val appConfig = ApplicationConfig.init("/conf/test.conf").getSourceConfig().get
      val generator = spy(new TrainingDataGenerator(appConfig, new SparkIo()))
      doReturn((200, Right(testData))).when(generator).getWeatherData(anyString)
      generator.extractData()
      generator.processData()
      val output = sparkSession.sql("select * from TrainingData")
      output.schema.toList.map(x => x.name).mkString(",") mustEqual expected

    }
  }

  "Method loadData in TrainingDataGenerator" should {
    "shoud write the output " in {
      import sparkSession.implicits._
      val sparkIo = spy(new SparkIo)
      val appConfig = ApplicationConfig.init("/conf/test.conf").getSourceConfig().get
      val generator = spy(new TrainingDataGenerator(appConfig, sparkIo))
      val dataframe = data.toDF("location", "latitude", "longitude", "elevation", "condition", "temperature", "humidity", "pressure", "day")
      dataframe.createOrReplaceTempView("TrainingData")
      generator.loadData()
      there was 4.times(sparkIo).writeTextFile(anyObject, anyString)(anyObject)
    }
  }
}