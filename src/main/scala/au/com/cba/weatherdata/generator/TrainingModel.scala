package au.com.cba.weatherdata.generator

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.rdd.RDD


class TrainingModel {

  def trainClassifier(training: RDD[LabeledPoint]): RandomForestModel = {
    val numClasses = 4
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numOfTrees = 10
    val featureSubsetStrategy = "auto"
    val impurity = "gini"
    val maxDepth = 4
    val maxBins = 32
    val model = RandomForest.trainClassifier(training, numClasses, categoricalFeaturesInfo,
      numOfTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
    model
  }

  def trainRegressor(training: RDD[LabeledPoint]): RandomForestModel = {
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numOfTrees = 5
    val featureSubsetStrategy = "auto"
    val impurity = "variance"
    val maxDepth = 4
    val maxBins = 32
    val model = RandomForest.trainRegressor(training, categoricalFeaturesInfo,
      numOfTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
    model
  }
}
