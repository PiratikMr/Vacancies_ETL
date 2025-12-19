package org.example.config.Loaders.modules

import org.example.config.Cases.CommonArgsConfig
import org.example.config.Loaders.ArgsLoader

trait WithCommonArgsConfig {
  self: ArgsLoader =>

  private val etlPart = opt[String]("etlpart", required = true)
  private val confFile = opt[String]("conffile", required = true)
  private val saveFolder = opt[String]("savefolder", default = Some("undefined"))

  lazy val common: CommonArgsConfig = {
    CommonArgsConfig(etlPart(), confFile(), saveFolder())
  }
}
