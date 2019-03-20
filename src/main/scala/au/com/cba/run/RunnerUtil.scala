package au.com.cba.run

import org.slf4j.LoggerFactory

object RunnerUtil {

  private val logger = LoggerFactory.getLogger(RunnerUtil.getClass)

  type OptionMap = Map[Symbol, String]

  def nextAdditionalOptions(optionsMap: OptionMap, args: List[String]): OptionMap = {
    args match {
      case Nil => optionsMap
      case "--app-name" :: value :: tail =>
        nextAdditionalOptions(optionsMap ++ Map('appName -> value.toString), tail)
      case "--output-count" :: value :: tail =>
        nextAdditionalOptions(optionsMap ++ Map('count -> value.toString), tail)
      case option :: _ => println("Unknown option " + option)
        sys.exit(1)
    }
  }
}