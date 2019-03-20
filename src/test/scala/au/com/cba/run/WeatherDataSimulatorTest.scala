package au.com.cba.run

import au.com.cba.testutil.TestUtils
import org.specs2.mock.Mockito

class WeatherDataSimulatorTest extends TestUtils with Mockito {

  "Method runSimulator in WeatherDataSimulator" should {
    "Throw assertion error when count is more than configured location" in {
      WeatherDataSimulator.runSimulator("", 100) must
        throwA[AssertionError]
    }
  }
}

