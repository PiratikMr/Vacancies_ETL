package com.Config

import com.typesafe.config.{Config, ConfigFactory}

trait LocalConfig {
  private lazy val defConfig: Config = ConfigFactory.load("Configuration.conf")
  private lazy val config = ConfigFactory.load("config.conf").withFallback(defConfig)

  protected def getStringField: String => String = config.getString
}
