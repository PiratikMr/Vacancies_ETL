ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "scala_transform",
    version := "0.1",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.3",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.3" % "provided",
    mainClass := Some("Transform")
  )


// sbt assembly
// scala test