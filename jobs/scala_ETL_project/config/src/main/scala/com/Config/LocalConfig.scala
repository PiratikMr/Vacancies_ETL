package com.Config

import org.rogach.scallop.{ScallopConf, ScallopOption}

/*trait LocalConfig {
  private lazy val defConfig: Config = ConfigFactory.load("Configuration.conf")
  private lazy val config = ConfigFactory.load("config.conf").withFallback(defConfig)

  protected def getStringField: String => String = config.getString
}*/

abstract class LocalConfig (args: Seq[String]) extends ScallopConf(args) {
  private val fileName: ScallopOption[String] = opt[String](name = "fileName", default = Some("Configuration.conf"))
  lazy val fileConf: ProjectConfig = new ProjectConfig(fileName())

  def define(): Unit = {
    verify()
    fileConf
  }
}
