package au.com.cba.common.util

import java.io.File
import java.net.{URL, URLClassLoader}

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object WeatherDataSimulatorUtil {
  private val log = LoggerFactory.getLogger(WeatherDataSimulatorUtil.getClass)

  def createSparkSession(appName: String, sparkRuntimeConf: Map[String, String] = Map.empty): SparkSession = {
    SparkSession
      .builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
  }

  def loadApplication(jarPath: String, appName: String) = {
    log.info("jarpath--" + jarPath)
    log.info("appName--" + appName)
    val classLoader = new URLClassLoader(Array[URL](
      new File(jarPath).toURI.toURL))
    val className = classLoader.loadClass(appName)
    className
  }
}
