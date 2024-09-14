ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.19"
val circeVersion = "0.14.4"


lazy val root = (project in file("."))
  .settings(
    name := "trageAggregation"
  ).dependsOn(dependencies)

//add kafka stream dependency with tests and testing framework

lazy val dependencies = (project in file("dependencies"))
  .settings(
    name := "dependencies",
    libraryDependencies ++= Seq(
      "org.apache.kafka" %% "kafka-streams-scala" % "3.8.0",
      "org.apache.kafka" % "kafka-streams-test-utils" % "3.8.0" % Test,
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
      "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-core" % "2.30.9",
      // Use the "provided" scope instead when the "compile-internal" scope is not supported
      "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % "2.30.9",
      "org.typelevel" %% "cats-core" % "2.12.0",
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "io.circe" %% "circe-generic-extras" % circeVersion,
      "org.apache.logging.log4j" %% "log4j-api-scala" % "13.1.0",
      "io.micrometer" % "micrometer-core" % "1.13.1",
      "org.apache.flink" %% "flink-scala" % "1.20.0",
      "org.apache.flink" % "flink-clients" % "1.20.0",
      "org.apache.flink" %% "flink-streaming-scala" % "1.20.0")




  )
libraryDependencies += "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.23.1"
libraryDependencies += "org.apache.flink" % "flink-statebackend-rocksdb" % "1.20.0"
// https://mvnrepository.com/artifact/org.apache.flink/flink-connector-kafka
libraryDependencies += "org.apache.flink" % "flink-connector-kafka" % "3.1.0-1.18"
libraryDependencies += "org.apache.flink" % "flink-core" % "1.20.0"
// https://mvnrepository.com/artifact/org.apache.flink/flink-connector-base
libraryDependencies += "org.apache.flink" % "flink-connector-base" % "1.20.0"
libraryDependencies += "org.apache.flink" % "flink-connector-aws-base" % "4.2.0-1.17"
// https://mvnrepository.com/artifact/org.apache.flink/flink-s3-fs-hadoop
libraryDependencies += "org.apache.flink" % "flink-s3-fs-hadoop" % "1.20.0" % "provided"
