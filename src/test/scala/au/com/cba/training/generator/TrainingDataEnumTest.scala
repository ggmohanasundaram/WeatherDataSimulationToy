package au.com.cba.training.generator

import au.com.cba.testutil.TestUtils
import org.specs2.mock.Mockito

class TrainingDataEnumTest extends TestUtils with Mockito {

  sequential

  import sparkSession.implicits._

  private val data = Seq(("some_data", "some_latitude", "some_longitude", "some_elevation", "some_day"),
    ("some_data", "some_latitude", "some_longitude", "some_elevation", "some_day"))

  private val expected = "[some_data 1:some_latitude 2:some_longitude 3:some_elevation 4:some_day]," +
    "[some_data 1:some_latitude 2:some_longitude 3:some_elevation 4:some_day]"

  "method generateTemperature should produce proper dataframe" in {
    val dataframe = data.toDF("temperature", "latitude", "longitude", "elevation", "day")

    dataframe.createOrReplaceTempView("test_temperature")
    val dataFrame = TrainingDataEnum.generateTemperature("test_temperature")

    val actual = dataFrame.collect().mkString(",")
    actual mustEqual expected
  }

  "method generateHumidity should produce proper dataframe" in {
    val dataframe = data
      .toDF("humidity", "latitude", "longitude", "elevation", "day")
    dataframe.createOrReplaceTempView("test_humidity")
    val dataFrame = TrainingDataEnum.generateHumidity("test_humidity")
    val actual = dataFrame.collect().mkString(",")
    actual mustEqual expected
  }

  "method generatePressure should produce proper dataframe" in {
    val dataframe = data
      .toDF("pressure", "latitude", "longitude", "elevation", "day")
    dataframe.createOrReplaceTempView("test_pressure")
    val dataFrame = TrainingDataEnum.generatePressure("test_pressure")
    val actual = dataFrame.collect().mkString(",")
    actual mustEqual expected
  }

  "method generateCondition should produce proper dataframe" in {
    val dataframe = data
      .toDF("condition", "latitude", "longitude", "elevation", "day")
    dataframe.createOrReplaceTempView("test_condition")
    val dataFrame = TrainingDataEnum.generateCondition("test_condition")
    val actual = dataFrame.collect().mkString(",")
    actual mustEqual expected
  }
}
