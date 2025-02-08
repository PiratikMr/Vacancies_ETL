package com.Config

import com.typesafe.config.Config

class APIConf(conf: Config, path: String) extends filePath(conf, path) {
  private lazy val accessToken: String = getString("accessToken")
  private lazy val userAgent: String = getString("userAgent")

  lazy val headers: Map[String, String] = Map(
    "Authorization" -> s"Bearer $accessToken",
    "User-Agent" -> userAgent
  )
}
