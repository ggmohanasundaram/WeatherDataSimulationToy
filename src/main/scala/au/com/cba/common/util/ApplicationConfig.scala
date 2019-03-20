package au.com.cba.common.util

import scala.io.Source
import scala.util.Try
import com.typesafe.config.{Config, ConfigFactory}

object ApplicationConfig {
  def init(confPath: String): ApplicationConfig = {
    val confURL = getClass.getResource(confPath)
    val confString = scala.io.Source.fromURL(confURL)
    new ApplicationConfig(confString.mkString)
  }

  def init(confPath: String, confType: String = "Internal"): ApplicationConfig = {
    val confString = Source.fromFile(confPath)
    new ApplicationConfig(confString.mkString)
  }
}

class ApplicationConfig(confString: String) {

  private val config: Config = ConfigFactory.parseString(confString)

  def getSourceConfig(): Option[SourceConfig] = {
    implicit val contextPath: String = "run.SourceConfig"
    val connectionURL = Try(config.getString(s"$contextPath.connectionURL")).toOption
    val inputPath = Try(config.getString(s"$contextPath.inputPath")).toOption
    val outputPath = Try(config.getString(s"$contextPath.outputPath")).toOption
    Some(SourceConfig(connectionURL, inputPath, outputPath))
  }
}

case class SourceConfig(connectionURL: Option[String], inputPath: Option[String], outputPath: Option[String])

