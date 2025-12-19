import sbt.Keys.libraryDependencies
import sbtassembly.AssemblyPlugin.autoImport.assembly

import scala.collection.Seq

lazy val sparkVersion = "4.0.0"

ThisBuild / version := "1"
ThisBuild / scalaVersion := "2.13.16"
ThisBuild / libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",

  "org.scalatest" %% "scalatest" % "3.2.18" % Test,
  "org.mockito" % "mockito-core" % "5.11.0" % Test,
  "org.scalatestplus" %% "mockito-5-10" % "3.2.18.0" % Test,
  "org.testcontainers" % "postgresql" % "1.21.3" % Test
)

lazy val assemblySettings = Seq(
  assembly / assemblyMergeStrategy := {
    case PathList("module-info.class") => MergeStrategy.discard
    case PathList("META-INF", "versions", xs @ _, "module-info.class") => MergeStrategy.discard
    case x => (assembly / assemblyMergeStrategy).value(x)
  }
)


lazy val infra_structure = (project in file("InfraStructure"))
  .settings(
    libraryDependencies ++= Seq(
      "com.typesafe" % "config" % "1.4.3",
      "org.rogach" %% "scallop" % "5.2.0",
      "com.softwaremill.sttp.client4" %% "core" % "4.0.10",
      "org.postgresql" % "postgresql" % "42.7.7",

      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
      "ch.qos.logback" % "logback-classic" % "1.5.13"
    )
  )

lazy val Currency = (project in file("Currency"))
  .settings(
    assemblySettings,
    assembly / mainClass := Some("org.example.currency.CurrencyMain"),
    assembly / assemblyJarName := "Currency-etl.jar"
  )
  .dependsOn(infra_structure)

lazy val Finder = (project in file("Finder"))
  .settings(
    assemblySettings,
    assembly / mainClass := Some("org.example.finder.FinderMain"),
    assembly / assemblyJarName := "Finder-etl.jar"
  )
  .dependsOn(infra_structure)

lazy val GeekJob = (project in file("GeekJob"))
  .settings(
    assemblySettings,
    libraryDependencies += "net.ruippeixotog" %% "scala-scraper" % "2.2.1",
    assembly / mainClass := Some("org.example.geekjob.GeekJobMain"),
    assembly / assemblyJarName := "GeekJob-etl.jar"
  )
  .dependsOn(infra_structure)

lazy val GetMatch = (project in file("GetMatch"))
  .settings(
    assemblySettings,
    assembly / mainClass := Some("org.example.getmatch.GetMatchMain"),
    assembly / assemblyJarName := "GetMatch-etl.jar"
  )
  .dependsOn(infra_structure)


lazy val HeadHunter = (project in file("HeadHunter"))
  .settings(
    assemblySettings,
    assembly / mainClass := Some("org.example.headhunter.HeadHunterMain"),
    assembly / assemblyJarName := "HeadHunter-etl.jar"
  )
  .dependsOn(infra_structure)

lazy val HeadHunterDictionaries = (project in file("HeadHunterDictionaries"))
  .settings(
    assemblySettings,
    assembly / mainClass := Some("org.example.headhunter.dictionaries.HeadHunterDictionariesMain"),
    assembly / assemblyJarName := "HeadHunterDictionaries-etl.jar"
  )
  .dependsOn(infra_structure)

lazy val HabrCareer = (project in file("HabrCareer"))
  .settings(
    assemblySettings,
    assembly / mainClass := Some("org.example.habrcareer.HabrCareerMain"),
    assembly / assemblyJarName := "HabrCareer-etl.jar"
  )
  .dependsOn(infra_structure)