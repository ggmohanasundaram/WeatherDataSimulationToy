name := "WeatherDataSimulationToy"
version := "0.1"
val sparkVersion = sys.env.getOrElse("SPARK_VERSION", "2.3.0")
parallelExecution in Test := false
organization := "au.com.cba"
scalaVersion := "2.11.11"


libraryDependencies ++= Seq( "com.typesafe" % "config" % "1.3.1",
    "com.softwaremill.sttp" %% "core" % "1.5.11",
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion ,
    "org.apache.spark" %% "spark-hive" % sparkVersion,
    "org.apache.spark" %% "spark-mllib" % sparkVersion,
    "org.specs2" %% "specs2-core" % "4.0.1" % Test,
    "org.specs2" %% "specs2-matcher-extra" % "4.0.1"% Test,
    "org.specs2" %% "specs2-mock" % "4.0.1" % Test)






