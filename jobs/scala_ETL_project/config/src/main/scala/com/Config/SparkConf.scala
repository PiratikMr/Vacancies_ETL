package com.Config

import com.typesafe.config.Config

class SparkConf(conf: Config, path: String ) extends filePath(conf, path) {
  lazy val name: String = getString("name")
  lazy val master: String = getString("master")
}
