import Dependencies._

ThisBuild / scalaVersion     := "2.12.11"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "transformers"
ThisBuild / organizationName := "transformers"


lazy val root = (project in file("."))
  .settings(
    name := "spark-etl-xml",
    libraryDependencies ++= Seq(
      scalaTest % Test,
      "com.github.fqaiser94" %% "mse" % "0.2.4",
      "za.co.absa" %% "spark-hofs" % "0.4.0",
      "org.apache.spark" %% "spark-core" % "3.0.1",
      "org.apache.spark" %% "spark-sql" % "3.0.1"
    )
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.

