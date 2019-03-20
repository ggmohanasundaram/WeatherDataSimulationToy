package au.com.cba.run

import au.com.cba.common.domain.Locations
import au.com.cba.common.util.{ApplicationConfig, SparkIo, WeatherDataSimulatorUtil}
import au.com.cba.training.generator.TrainingDataGenerator
import au.com.cba.weatherdata.generator.WeatherDataGenerator
import org.apache.commons.lang.exception.ExceptionUtils
import org.slf4j.LoggerFactory

object WeatherDataSimulator {

  private val log = LoggerFactory.getLogger(WeatherDataSimulator.getClass)

  def main(args: Array[String]): Unit = {
    val additionalOptions = RunnerUtil.nextAdditionalOptions(Map(), args.toList)
    val appName = additionalOptions.get('appName).getOrElse("WeatherDataGenerator")
    val count = additionalOptions.get('count).getOrElse("10").toInt
    runSimulator(appName, count)
  }

  def runSimulator(appName: String, count: Int) = {
    implicit val sparkSession = WeatherDataSimulatorUtil.createSparkSession(appName)
    try {
      log.info(s"WeatherDataSimulator is running $appName $count")
      assert(count <= Locations.locations.size, s"Please provide the number of Records is less than equal to ${Locations.locations.size}")

      val sparkIo: SparkIo = new SparkIo
      val generator = appName match {
        case "TrainingDataGenerator" => new TrainingDataGenerator(ApplicationConfig.init("/conf/trainingdata.conf").getSourceConfig().get, sparkIo)
        case _ => new WeatherDataGenerator(ApplicationConfig.init("/conf/weatherdata.conf").getSourceConfig().get, sparkIo, count = count)
      }
      generator.extractData()
      generator.processData()
      generator.loadData()
    }
    catch {
      case e: Exception => log.error(s"Exception in WeatherDataSimulator ${ExceptionUtils.getStackTrace(e)}")
    }
    finally {
      sparkSession.stop()
      // sbt "run  --app-name  TrainingDataGenerator"
    }
  }
}
