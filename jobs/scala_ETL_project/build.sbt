import sbt.Keys.libraryDependencies
import sbtassembly.AssemblyPlugin.autoImport.assembly

import scala.collection.Seq

lazy val sparkVersion = "4.0.0"

ThisBuild / version := "1"
ThisBuild / scalaVersion := "2.13.16"
ThisBuild / libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)


val extract: String = "extract"
val transform: String = "transform"
val load: String = "load"
val update: String = "update"


// InfraStructure/
lazy val inf_struct = "InfraStructure/"

  lazy val Config = (project in file(inf_struct + "Config")).settings(
    libraryDependencies += "com.typesafe" % "config" % "1.4.3",
    libraryDependencies += "org.rogach" %% "scallop" % "5.2.0"
  )

  lazy val Core = (project in file (inf_struct + "Core")).dependsOn(Config)

  lazy val URLInteraction = (project in file(inf_struct + "URLInteraction"))
    .settings(
      libraryDependencies += "com.softwaremill.sttp.client4" %% "core" % "4.0.10"
    ).dependsOn(Config)

  lazy val DBInteraction = (project in file(inf_struct + "DBInteraction"))
    .settings(
      libraryDependencies += "org.postgresql" % "postgresql" % "42.7.7"
    ).dependsOn(Core)


// HeadHunter/
lazy val hh_dir = "HeadHunter/"

  // Dictionaries/
  lazy val hh_dict = hh_dir + "Dictionaries/"

    lazy val hh_dict_extract = (project in file(hh_dict + extract))
      .settings(
        assembly / mainClass := Some("com.files.ExtractDictionaries"),
        assembly / assemblyJarName := s"$extract.jar"
      )
      .dependsOn(URLInteraction, Core)

    lazy val hh_dict_load = (project in file(hh_dict + load))
      .settings(
        assembly / mainClass := Some("com.files.LoadDictionaries"),
        assembly / assemblyJarName := s"$load.jar"
      )
      .dependsOn(DBInteraction)
  //

  // Vacancies/
  lazy val hh_vacs = hh_dir + "Vacancies/"

    lazy val hh_vacs_extract = (project in file(hh_vacs + extract))
        .settings(
          assembly / mainClass := Some("com.files.ExtractVacancies"),
          assembly / assemblyJarName := s"$extract.jar",
          assembly / assemblyShadeRules := Seq(
            ShadeRule.rename("org.typelevel.cats.**" -> "repackaged.org.typelevel.cats.@1").inAll,
            ShadeRule.rename("cats.**" -> "repackaged.cats.@1").inAll,
          )
        )
        .dependsOn(URLInteraction, Core)

    lazy val hh_vacs_transform = (project in file(hh_vacs + transform))
      .settings(
        assembly / mainClass := Some("com.files.TransformVacancies"),
        assembly / assemblyJarName := s"$transform.jar"
      )
      .dependsOn(Core)

    lazy val hh_vacs_load = (project in file(hh_vacs + load))
      .settings(
        assembly / mainClass := Some("com.files.LoadVacancies"),
        assembly / assemblyJarName := s"$load.jar"
      )
      .dependsOn(DBInteraction)

    lazy val hh_vacs_update = (project in file(hh_vacs + update))
      .settings(
        assembly / mainClass := Some("com.files.UpdateVacancies"),
        assembly / assemblyJarName := "update.jar"
      )
      .dependsOn(URLInteraction, DBInteraction)
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
    .dependsOn(URLInteraction, Core)

  lazy val gm_vacs_transform = (project in file(gm_vacs + transform))
    .settings(
      assembly / mainClass := Some("com.files.TransformVacancies"),
      assembly / assemblyJarName := s"$transform.jar"
    )
    .dependsOn(Core, DBInteraction)

  lazy val gm_vacs_load = (project in file(gm_vacs + load))
    .settings(
      assembly / mainClass := Some("com.files.LoadVacancies"),
      assembly / assemblyJarName := s"$load.jar"
    )
    .dependsOn(DBInteraction)

  lazy val gm_vacs_update = (project in file(gm_vacs + update))
    .settings(
      assembly / mainClass := Some("com.files.UpdateVacancies"),
      assembly / assemblyJarName := s"$update.jar"
    ).dependsOn(DBInteraction, URLInteraction)
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
    .dependsOn(URLInteraction, Core)

  lazy val gj_vacs_transform = (project in file(gj_vacs + transform))
    .settings(
      libraryDependencies += "net.ruippeixotog" %% "scala-scraper" % "2.2.1",
      assembly / mainClass := Some("com.files.TransformVacancies"),
      assembly / assemblyJarName := s"$transform.jar",
      assembly / assemblyMergeStrategy := {
        case PathList("module-info.class") => MergeStrategy.discard
        case x => (assembly / assemblyMergeStrategy).value(x)
      }
    )
    .dependsOn(DBInteraction, Core)

  lazy val gj_vacs_load = (project in file(gj_vacs + load))
    .settings(
      assembly / mainClass := Some("com.files.LoadVacancies"),
      assembly / assemblyJarName := s"$load.jar"
    )
    .dependsOn(DBInteraction, Core)

  lazy val gj_vacs_update = (project in file(gj_vacs + update))
    .settings(
      assembly / mainClass := Some("com.files.UpdateVacancies"),
      assembly / assemblyJarName := s"$update.jar"
    ).dependsOn(URLInteraction, DBInteraction)
  //
//

// Finder/
lazy val fn_dir = "Finder/"
// Vacancies
lazy val fn_vacs = fn_dir + "Vacancies/"

lazy val fn_vacs_extract = (project in file(fn_vacs + extract))
  .settings(
    assembly / mainClass := Some("com.files.ExtractVacancies"),
    assembly / assemblyJarName := s"$extract.jar"
  )
  .dependsOn(URLInteraction, Core)

lazy val fn_vacs_transform = (project in file(fn_vacs + transform))
  .settings(
    assembly / mainClass := Some("com.files.TransformVacancies"),
    assembly / assemblyJarName := s"$transform.jar"
  )
  .dependsOn(DBInteraction, Core)

lazy val fn_vacs_load = (project in file(fn_vacs + load))
  .settings(
    assembly / mainClass := Some("com.files.LoadVacancies"),
    assembly / assemblyJarName := s"$load.jar"
  )
  .dependsOn(DBInteraction, Core)

lazy val fn_vacs_update = (project in file(fn_vacs + update))
  .settings(
    assembly / mainClass := Some("com.files.UpdateVacancies"),
    assembly / assemblyJarName := s"$update.jar"
  )
  .dependsOn(URLInteraction, DBInteraction)
//
//


lazy val root = (project in file("."))
  .aggregate(Config, Core, URLInteraction, DBInteraction,
    hh_dict_extract, hh_dict_load,
    hh_vacs_extract, hh_vacs_transform, hh_vacs_load, hh_vacs_update,
    gm_vacs_extract, gm_vacs_transform, gm_vacs_load, gm_vacs_update,
    gj_vacs_extract, gj_vacs_transform, gj_vacs_load, gj_vacs_update,
    fn_vacs_extract, fn_vacs_transform, fn_vacs_load, fn_vacs_update,
  )


lazy val infra_structure = (project in file("InfraStructure"))
  .aggregate(Config, Core, URLInteraction, DBInteraction)


lazy val head_hunter = (project in file("HeadHunter"))
  .aggregate(
    hh_dict_extract, hh_dict_load,
    hh_vacs_extract, hh_vacs_transform, hh_vacs_load, hh_vacs_update
  )

lazy val get_match = (project in file("GetMatch"))
  .aggregate(
    gm_vacs_extract, gm_vacs_transform, gm_vacs_load, gm_vacs_update
  )

lazy val geek_job = (project in file("GeekJOB"))
  .aggregate(
    gj_vacs_extract, gj_vacs_transform, gj_vacs_load, gj_vacs_update
  )

lazy val finder = (project in file("Finder"))
  .aggregate(
    fn_vacs_extract, fn_vacs_transform, fn_vacs_load, fn_vacs_update
  )




// gm_helper
addCommandAlias("gmHelperCompile",
  """
    |project get_match; clean; compile;
    |project gm_vacs_extract; assembly;
    |project gm_vacs_transform; assembly;
    |project gm_vacs_load; assembly;
    |project gm_vacs_update; assembly;
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
    |project gj_vacs_update; assembly;
    |project root;
    |""".stripMargin
)

// hh_Helper
addCommandAlias("hhHelperCompile",
  """
    |project head_hunter; clean; compile;
    |project hh_dict_extract; assembly;
    |project hh_dict_load; assembly;
    |project hh_vacs_extract; assembly;
    |project hh_vacs_transform; assembly;
    |project hh_vacs_load; assembly;
    |project hh_vacs_update; assembly;
    |project root;
    |""".stripMargin
)

// fn_helper
addCommandAlias("fnHelperCompile",
  """
    |project finder; clean; compile;
    |project fn_vacs_extract; assembly;
    |project fn_vacs_transform; assembly;
    |project fn_vacs_load; assembly;
    |project fn_vacs_update; assembly;
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

// Finder
addCommandAlias("compileFn",
  """
    |project infra_structure; clean; compile;
    |fnHelperCompile;
    |""".stripMargin
)


// Whole Project
addCommandAlias("compileWholeProj",
    """
    |project Config; clean; compile;
    |project Core; clean; compile;
    |project infra_structure; clean; compile;
    |gmHelperCompile;
    |hhHelperCompile;
    |gjHelperCompile;
    |fnHelperCompile;
    |""".stripMargin
)