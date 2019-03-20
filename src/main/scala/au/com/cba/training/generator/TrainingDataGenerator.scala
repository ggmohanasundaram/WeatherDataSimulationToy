package au.com.cba.training.generator

import au.com.cba.common.domain.{Locations, WeatherDataEnum}
import au.com.cba.common.generator.DataGenerator
import au.com.cba.common.util.{SourceConfig, SparkIo}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions._
import com.softwaremill.sttp.{sttp, _}

class TrainingDataGenerator(override val sourceConfig: SourceConfig, override val sparkIo: SparkIo,
                            override val inputName: Option[String] = None, override val outputName: Option[String] = Some("TrainingData")) extends DataGenerator {


  val outputTable = outputName.getOrElse("RawWeatherData")

  private val log = LoggerFactory.getLogger(classOf[TrainingDataGenerator])

  override def extractData()(implicit sparkSession: SparkSession) = {
    try {
      val base_url = sourceConfig.connectionURL.get
      log.info(s"get Exchange Rate Info from $base_url")
      val dataframe: List[Option[DataFrame]] = Locations.locations.map(
        location => {
          val response = getWeatherData(s"$base_url/${location.latitude},${location.longitude}")
          response._1 match {
            case 200 =>
              val value = response._2
              val weatherData = sparkIo.loadJsonFromString(value.right.get).
                withColumn("location", lit(location.name)).
                withColumn("elevation", lit(location.elevation))
              weatherData.createOrReplaceTempView("weatherData")
              val exchangeRate = sparkSession.sql(
                """
                  |select location,elevation,latitude,longitude,hourly.data.time as time ,hourly.data.precipType as precipType
                  |,hourly.data.temperature as temperature, hourly.data.humidity as humidity
                  |,hourly.data.pressure as pressure from weatherData
                """.stripMargin)
              Some(exchangeRate)
            case _ => log.info("No response From Source")
              None
          }
        }
      )
      val frames = dataframe.filter(_.isDefined).map(_.get)

      val mergedDataFrame = frames.isEmpty match {
        case true => sparkSession.emptyDataFrame
        case false => frames.reduceLeft(_ union _)
      }

      mergedDataFrame.createOrReplaceTempView(outputTable)
    }
    catch {
      case e: Exception => throw e
    }
  }

  override def processData()(implicit sparkSession: SparkSession): Unit = {
    try {

      val time = sparkSession.sql(
        s"""select location,latitude,longitude,explode(time)
           | as datetime from $outputTable""".stripMargin).withColumn("id", monotonically_increasing_id())

      val condition = sparkSession.sql(
        s"""select location,elevation,latitude,longitude,explode(precipType)
           | as condition  from $outputTable""".stripMargin).withColumn("id", monotonically_increasing_id())

      val temperature = sparkSession.sql(
        s"""select location,latitude,longitude,explode(temperature)
           | as temperature  from $outputTable""".stripMargin).withColumn("id", monotonically_increasing_id())

      val humidity = sparkSession.sql(
        s"""select location,latitude,longitude,explode(humidity)
           |  as humidity  from $outputTable""".stripMargin).withColumn("id", monotonically_increasing_id())

      val pressure = sparkSession.sql(
        s"""select location,latitude,longitude,explode(pressure)
           | as pressure  from $outputTable""".stripMargin).withColumn("id", monotonically_increasing_id())


      condition.createOrReplaceTempView("condition")
      temperature.createOrReplaceTempView("temperature")
      humidity.createOrReplaceTempView("humidity")
      pressure.createOrReplaceTempView("pressure")
      time.createOrReplaceTempView("time")

      val sql =
        """
          |select c.location,c.latitude,c.longitude,c.elevation,
          |case
          |when c.condition like '%rain%' then 1.0
          |when c.condition like '%sunny%' then 2.0
          |else 3.0
          |end as condition, t.temperature,h.humidity,p.pressure,DAYOFWEEK(cast(from_unixtime(ti.datetime) as Date)) as day from condition c
          |left  join temperature t on c.latitude =t.latitude  and c.longitude =t.longitude and c.id= t.id
          |left join humidity h on c.latitude =h.latitude  and c.longitude =h.longitude and c.id = h.id
          |left join pressure p on c.latitude =p.latitude  and c.longitude =p.longitude and c.id = p.id
          |left join time ti on c.latitude =ti.latitude  and c.longitude =ti.longitude  and c.id = ti.id

        """.stripMargin
      sparkSession.sql(sql).createOrReplaceTempView(outputTable)
    }
    catch {
      case e: Exception => throw e
    }
  }

  override def loadData()(implicit sparkSession: SparkSession): Unit = {

    WeatherDataEnum.values.map(data => sparkIo.writeTextFile(TrainingDataEnum.generateTrainingData(TrainingDataEnum.fromString(data.toString), outputTable),
      s"${sourceConfig.outputPath.get}/${data.toString.toLowerCase}.txt"))

  }


  private[generator] def getWeatherData(urlString: String) = {
    try {
      val sort: Option[String] = None
      val query = "http language:scala"
      sttp.get(uri"${urlString}")
      implicit val backend = HttpURLConnectionBackend()
      val request = sttp.get(uri"$urlString")
      val response = request.send()
      (response.code, response.body)
    }
    catch {
      case e: Exception => throw e
    }
  }
}


