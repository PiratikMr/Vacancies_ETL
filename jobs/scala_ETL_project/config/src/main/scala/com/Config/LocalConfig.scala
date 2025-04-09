package com.Config

import org.rogach.scallop.{ScallopConf, ScallopOption}

abstract class LocalConfig (args: Seq[String], apiHeader: String = "") extends ScallopConf(args) {
  private val fileName: ScallopOption[String] = opt[String](name = "fileName", default = Some("Configuration.conf"))
  private val date: ScallopOption[String] = opt[String](name = "date", default = Some(null))
  lazy val fileConf: ProjectConfig = new ProjectConfig(fileName(), apiHeader, date())

  def define(): Unit = {
    verify()
    fileConf
  }
}
