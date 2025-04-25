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

val extract: String = "extract"
val transform: String = "transform"
val load: String = "load"


// InfraStructure/
lazy val inf_struct = "InfraStructure/"

  lazy val config = (project in file(inf_struct + "config")).settings(
    libraryDependencies += "com.typesafe" % "config" % "1.4.3",
    libraryDependencies += "org.rogach" %% "scallop" % "5.2.0"
  )

  lazy val core = (project in file (inf_struct + "core"))
    .dependsOn(config)

  lazy val urlExtract = (project in file(inf_struct + "URLExtract"))
    .settings(
      libraryDependencies += "com.softwaremill.sttp.client3" %% "core" % "3.10.1"
    ).dependsOn(config)

  lazy val DBLoad = (project in file(inf_struct + "DBLoad"))
    .settings(
      libraryDependencies += "org.postgresql" % "postgresql" % "42.7.4"
    ).dependsOn(core)
//

// HeadHunter/
lazy val hh_dir = "HeadHunter/"
  // Currency/
  lazy val hh_curr = hh_dir + "Currency/"

    lazy val hh_curr_extract = (project in file(hh_curr + extract))
      .settings(
        assembly / mainClass := Some("com.files.ExtractCurrency"),
        assembly / assemblyJarName := s"$extract.jar"
      )
      .dependsOn(urlExtract, core)

    lazy val hh_curr_load = (project in file(hh_curr + load))
      .settings(
        assembly / mainClass := Some("com.files.LoadCurrency"),
        assembly / assemblyJarName := s"$load.jar"
      )
      .dependsOn(DBLoad)
  //

  // Dictionaries/
  lazy val hh_dict = hh_dir + "Dictionaries/"

    lazy val hh_dict_extract = (project in file(hh_dict + extract))
      .settings(
        assembly / mainClass := Some("com.files.ExtractDictionaries"),
        assembly / assemblyJarName := s"$extract.jar"
      )
      .dependsOn(urlExtract, core)

    lazy val hh_dict_load = (project in file(hh_dict + load))
      .settings(
        assembly / mainClass := Some("com.files.LoadDictionaries"),
        assembly / assemblyJarName := s"$load.jar"
      )
      .dependsOn(DBLoad)
  //

  // Vacancies/
  lazy val hh_vacs = hh_dir + "Vacancies/"

    lazy val hh_vacs_extract = (project in file(hh_vacs + extract))
        .settings(
          assembly / mainClass := Some("com.files.ExtractVacancies"),
          assembly / assemblyJarName := s"$extract.jar"
        )
        .dependsOn(urlExtract, core)

    lazy val hh_vacs_transform = (project in file(hh_vacs + transform))
      .settings(
        assembly / mainClass := Some("com.files.TransformVacancies"),
        assembly / assemblyJarName := s"$transform.jar"
      )
      .dependsOn(core)

    lazy val hh_vacs_load = (project in file(hh_vacs + load))
      .settings(
        assembly / mainClass := Some("com.files.LoadVacancies"),
        assembly / assemblyJarName := s"$load.jar"
      )
      .dependsOn(DBLoad)
  //
//

// GetMatch/
lazy val gm_dir = "GetMatch/"
  // Vacancies/
  lazy val gm_vacs = gm_dir + "Vacancies/"

  lazy val gm_vacs_extract = (project in file(gm_vacs + extract))
    .settings(
      assembly / mainClass := Some("com.files.ExtractVacancies"),
      assembly / assemblyJarName := s"$extract.jar"
    )
    .dependsOn(urlExtract, core)

  lazy val gm_vacs_transform = (project in file(gm_vacs + transform))
    .settings(
      assembly / mainClass := Some("com.files.TransformVacancies"),
      assembly / assemblyJarName := s"$transform.jar"
    )
    .dependsOn(core, DBLoad)

  lazy val gm_vacs_load = (project in file(gm_vacs + load))
    .settings(
      assembly / mainClass := Some("com.files.LoadVacancies"),
      assembly / assemblyJarName := s"$load.jar"
    )
    .dependsOn(DBLoad)
  //
//

// GeekJOB/
lazy val gj_dir = "GeekJOB/"
  // Vacancies
  lazy val gj_vacs = gj_dir + "Vacancies/"

  lazy val gj_vacs_extract = (project in file(gj_vacs + extract))
    .settings(
      assembly / mainClass := Some("com.files.ExtractVacancies"),
      assembly / assemblyJarName := s"$extract.jar"
    )
    .dependsOn(urlExtract, core)

  lazy val gj_vacs_transform = (project in file(gj_vacs + transform))
    .settings(
      libraryDependencies += "net.ruippeixotog" %% "scala-scraper" % "2.2.1",
      assembly / mainClass := Some("com.files.TransformVacancies"),
      assembly / assemblyJarName := s"$transform.jar"
    )
    .dependsOn(DBLoad, core)

  lazy val gj_vacs_load = (project in file(gj_vacs + load))
    .settings(
      assembly / mainClass := Some("com.files.LoadVacancies"),
      assembly / assemblyJarName := s"$load.jar"
    )
    .dependsOn(DBLoad)
  //
//


lazy val root = (project in file("."))
  .aggregate(config, core, urlExtract, DBLoad,
    hh_curr_extract, hh_curr_load,
    hh_dict_extract, hh_dict_load,
    hh_vacs_extract, hh_vacs_transform, hh_vacs_load,
    gm_vacs_extract, gm_vacs_transform, gm_vacs_load,
    gj_vacs_extract, gj_vacs_transform
  )


lazy val infra_structure = (project in file("InfraStructure"))
  .aggregate(config, core, urlExtract, DBLoad)


lazy val head_hunter = (project in file("HeadHunter"))
  .aggregate(
    hh_curr_extract, hh_curr_load,
    hh_dict_extract, hh_dict_load,
    hh_vacs_extract, hh_vacs_transform, hh_vacs_load
  )

lazy val get_match = (project in file("GetMatch"))
  .aggregate(
    gm_vacs_extract, gm_vacs_transform, gm_vacs_load
  )

lazy val geek_job = (project in file("GeekJOB"))
  .aggregate(
    gj_vacs_extract, gj_vacs_transform, gj_vacs_load
  )




// gm_helper
addCommandAlias("gmHelperCompile",
  """
    |project get_match; clean; compile;
    |project gm_vacs_extract; assembly;
    |project gm_vacs_transform; assembly;
    |project gm_vacs_load; assembly;
    |project root;
    |""".stripMargin
)

// gj_helper
addCommandAlias("gjHelperCompile",
  """
    |project geek_job; clean; compile;
    |project gj_vacs_extract; assembly;
    |project gj_vacs_transform; assembly;
    |project gj_vacs_load; assembly;
    |project root;
    |""".stripMargin
)

// hh_Helper
addCommandAlias("hhHelperCompile",
  """
    |project head_hunter; clean; compile;
    |project hh_curr_extract; assembly;
    |project hh_curr_load; assembly;
    |project hh_dict_extract; assembly;
    |project hh_dict_load; assembly;
    |project hh_vacs_extract; assembly;
    |project hh_vacs_transform; assembly;
    |project hh_vacs_load; assembly;
    |project root;
    |""".stripMargin
)


// HeadHunter
addCommandAlias("compileHH",
  """
    |project infra_structure; clean; compile;
    |hhHelperCompile;
    |""".stripMargin
)

// GetMatch
addCommandAlias("compileGM",
  """
    |project infra_structure; clean; compile;
    |gmHelperCompile;
    |""".stripMargin
)

// GeekJob
addCommandAlias("compileGJ",
  """
    |project infra_structure; clean; compile;
    |gjHelperCompile;
    |""".stripMargin
)


// Whole Project
addCommandAlias("compileWholeProj",
    """
    |project infra_structure; clean; compile;
    |gmHelperCompile;
    |hhHelperCompile;
    |gjHelperCompile;
    |""".stripMargin
)



// $ export JAVA_OPTS='--add-exports java.base/sun.nio.ch=ALL-UNNAMED'