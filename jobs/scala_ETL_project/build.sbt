import sbt.Keys.libraryDependencies
import sbtassembly.AssemblyPlugin.autoImport.assembly

import scala.collection.Seq

lazy val sparkVersion = "3.5.4"

ThisBuild / version := "1"
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)

// Base/
lazy val baseDir = "Base/"

  lazy val config = (project in file(baseDir + "config")).settings(
    libraryDependencies += "com.typesafe" % "config" % "1.4.3",
    libraryDependencies += "org.rogach" %% "scallop" % "5.2.0"
  )

  lazy val core = (project in file (baseDir + "core"))
    .dependsOn(config)
//

// InfraStructure/
lazy val inStrDir = "InfraStructure/"

  lazy val urlExtract = (project in file(inStrDir + "URLExtract"))
    .settings(
      libraryDependencies += "com.softwaremill.sttp.client3" %% "core" % "3.10.1"
    ).dependsOn(config)

  lazy val DBLoad = (project in file(inStrDir + "DBLoad"))
    .settings(
      libraryDependencies += "org.postgresql" % "postgresql" % "42.7.4"
    ).dependsOn(core)
//

// HeadHunter/
lazy val hhDir = "HeadHunter/"
  // Currency/
  lazy val currDir = hhDir + "Currency/"

    lazy val currExtract = (project in file(currDir + "extract"))
      .settings(
        assembly / mainClass := Some("com.files.ExtractCurrency"),
        assembly / assemblyJarName := "extract.jar"
      )
      .dependsOn(urlExtract, core)

    lazy val currLoad = (project in file(currDir + "load"))
      .settings(
        assembly / mainClass := Some("com.files.LoadCurrency"),
        assembly / assemblyJarName := "load.jar"
      )
      .dependsOn(DBLoad)
  //

  // Dictionaries/
  lazy val dictDir = hhDir + "Dictionaries/"

    lazy val dictExtract = (project in file(dictDir + "extract"))
      .settings(
        assembly / mainClass := Some("com.files.ExtractDictionaries"),
        assembly / assemblyJarName := "extract.jar"
      )
      .dependsOn(urlExtract, core)

    lazy val dictLoad = (project in file(dictDir + "load"))
      .settings(
        assembly / mainClass := Some("com.files.LoadDictionaries"),
        assembly / assemblyJarName := "load.jar"
      )
      .dependsOn(DBLoad)
  //

  // Vacancies/
  lazy val vacDir = hhDir + "Vacancies/"

    lazy val vacExtract = (project in file(vacDir + "extract"))
        .settings(
          assembly / mainClass := Some("com.files.ExtractVacancies"),
          assembly / assemblyJarName := "extract.jar"
        )
        .dependsOn(urlExtract, core)

    lazy val vacTransform = (project in file(vacDir + "transform"))
      .settings(
        assembly / mainClass := Some("com.files.TransformVacancies"),
        assembly / assemblyJarName := "transform.jar"
      )
      .dependsOn(core)

    lazy val vacLoad = (project in file(vacDir + "load"))
      .settings(
        assembly / mainClass := Some("com.files.LoadVacancies"),
        assembly / assemblyJarName := "load.jar"
      )
      .dependsOn(DBLoad)
  //
//

// GetMatch/
lazy val gmDir = "GetMatch/"
  // Vacancies/
  lazy val gvacDir = gmDir + "Vacancies/"

  lazy val gvacExtract = (project in file(gvacDir + "extract"))
    .settings(
      assembly / mainClass := Some("com.files.ExtractVacancies"),
      assembly / assemblyJarName := "extract.jar"
    )
    .dependsOn(urlExtract, core)

  lazy val gvacTransform = (project in file(gvacDir + "transform"))
    .settings(
      assembly / mainClass := Some("com.files.TransformVacancies"),
      assembly / assemblyJarName := "transform.jar"
    )
    .dependsOn(core)

  lazy val gvacLoad = (project in file(gvacDir + "load"))
    .settings(
      assembly / mainClass := Some("com.files.LoadVacancies"),
      assembly / assemblyJarName := "load.jar"
    )
    .dependsOn(DBLoad)
  //
//


lazy val root = (project in file("."))
  .aggregate(config, core, urlExtract, DBLoad,
    currExtract, currLoad,
    dictExtract, dictLoad,
    vacExtract, vacTransform, vacLoad,
    gvacExtract, gvacTransform, gvacLoad
  )

addCommandAlias("gmHelperCompile",
  """
    |project gvacExtract; compile; assembly;
    |project gvacTransform; compile; assembly;
    |project gvacLoad; compile; assembly;
    |project root;
    |""".stripMargin
)

addCommandAlias("hhHelperCompile",
  """
    |project currExtract; compile; assembly;
    |project currLoad; compile; assembly;
    |project dictExtract; compile; assembly;
    |project dictLoad; compile; assembly;
    |project vacExtract; compile; assembly;
    |project vacTransform; compile; assembly;
    |project vacLoad; compile; assembly;
    |project root;
    |""".stripMargin
)

addCommandAlias("compileHH",
  """
    |project root; clean; compile;
    |hhHelperCompile;
    |""".stripMargin
)

addCommandAlias("compileGM",
  """
    |project root; clean; compile;
    |gmHelperCompile;
    |""".stripMargin
)

addCommandAlias("compileWholeProj",
    """
    |project root; clean; compile;
    |gmHelperCompile;
    |hhHelperCompile;
    |""".stripMargin
)



// $ export JAVA_OPTS='--add-exports java.base/sun.nio.ch=ALL-UNNAMED'