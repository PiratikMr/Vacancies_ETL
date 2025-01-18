package com.Config

import com.typesafe.config.{Config, ConfigFactory}

trait LocalConfig {
  private lazy val config: Config = ConfigFactory.parseResources("config/config.conf")
  val getString: String => String = config.getString
}
