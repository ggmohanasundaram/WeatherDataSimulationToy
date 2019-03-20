package au.com.cba.util

import au.com.cba.common.util.{ApplicationConfig, SourceConfig}
import org.specs2.mutable.Specification


class ApplicationConfigTest extends Specification {

  "ApplicationConfig" should {
    "process source configuration from internal conf" in {
      val applicationConfig = ApplicationConfig.init("/conf/test.conf")
      val expected = SourceConfig(Some("someUrl"), Some("someInputpath"), Some("someOutputpath"))
      applicationConfig.getSourceConfig().get mustEqual (expected)
    }
    "process source configuration from external conf" in {
      val parrentPath = System.getProperty("user.dir")
      val applicationConfig = ApplicationConfig.init(s"$parrentPath/src/test/resources/conf/test.conf","")
      val expected = SourceConfig(Some("someUrl"), Some("someInputpath"), Some("someOutputpath"))
      applicationConfig.getSourceConfig().get mustEqual (expected)
    }
  }
}