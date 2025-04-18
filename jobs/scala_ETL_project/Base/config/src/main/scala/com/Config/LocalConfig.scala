package com.Config

import com.Config.FolderName.FolderName
import org.rogach.scallop.{ScallopConf, ScallopOption}

abstract class LocalConfig (args: Seq[String]) extends ScallopConf(args) {
  private val fileName: ScallopOption[String] = opt[String](name = "fileName", default = Some("jobs/scala_ETL_project/Configuration/Configuration.conf"))
  private val date: ScallopOption[String] = opt[String](name = "date", default = Some(null))
  private val site: ScallopOption[String] = opt[String](name = "site")

  lazy val fileConf: ProjectConfig = new ProjectConfig(fileName(), site(), date())

  def tableName(folderName: FolderName): String = {
   s"${site()}_$folderName"
  }

  def define(): Unit = {
    verify()
    fileConf
  }
}
