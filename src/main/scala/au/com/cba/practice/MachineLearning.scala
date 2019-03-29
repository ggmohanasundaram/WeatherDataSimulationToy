package au.com.cba.practice

import au.com.cba.common.util.WeatherDataSimulatorUtil
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.Row

object MachineLearning {

  def main(args: Array[String]): Unit = {
    val sparkSession = WeatherDataSimulatorUtil.createSparkSession("MachineLearning")
    try {
      import sparkSession.implicits._

      val training = sparkSession.createDataFrame(Seq(
        (1.0, Vectors.dense(0.0, 1.1, 0.1)),
        (0.0, Vectors.dense(2.0, 1.0, -1.0)),
        (0.0, Vectors.dense(2.0, 1.3, 1.0)),
        (1.0, Vectors.dense(0.0, 1.2, -0.5))
      )).toDF("label", "features")

      val lr = new LogisticRegression()

      println(s"LogisticRegression parameters:\n ${lr.explainParams()}\n")

      val model1 = lr.fit(training)

      val test = sparkSession.createDataFrame(Seq(
        (1.0, Vectors.dense(-1.0, 1.5, 1.3)),
        (0.0, Vectors.dense(3.0, 2.0, -0.1)),
        (1.0, Vectors.dense(0.0, 2.2, -1.5))
      )).toDF("label", "features")

      model1.transform(test)
        .select("features", "label", "probability", "prediction")
        .collect()
        .foreach { case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>
          println(s"($features, $label) -> prob=$prob, prediction=$prediction")

        }
    }
    catch {
      case e: Exception => throw e
    }
    finally {
      sparkSession.stop()
    }

  }
}
