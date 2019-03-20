package au.com.cba.weatherdata.generator

import au.com.cba.common.util.{ApplicationConfig, SourceConfig, SparkIo}
import au.com.cba.testutil.TestUtils
import org.specs2.mock.Mockito

class WeatherDataGeneratorTest extends TestUtils with Mockito {

  "Method extractData in WeatherDataGenerator" should {
    "write empty dataframe for no responses" in {
      val parrentPath = System.getProperty("user.dir")
      val config = SourceConfig(Some(""),Some(s"$parrentPath/src/test/resources/testData/"),None)
      val generator = spy(new WeatherDataGenerator(config, new SparkIo()))
      generator.extractData()
      generator.models.size mustEqual(4)
    }
  }
}