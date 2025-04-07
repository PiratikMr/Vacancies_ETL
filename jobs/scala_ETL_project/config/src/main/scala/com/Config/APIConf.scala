package com.Config

import com.typesafe.config.Config

import scala.jdk.CollectionConverters.asScalaSetConverter

class APIConf(conf: Config) extends Serializable {
  /*private lazy val accessToken: String = getString("accessToken")
  private lazy val userAgent: String = getString("userAgent")

  lazy val headers: Map[String, String] = Map(
    "Authorization" -> s"Bearer $accessToken",
    "User-Agent" -> userAgent
  )*/

  private val headersConfig: Config = conf.getConfig("Headers")
  val headers: Map[String, String] = headersConfig.entrySet().asScala.map { entry =>
    val key = entry.getKey
    val value = headersConfig.getString(key)
    key -> value
  }.toMap
}
