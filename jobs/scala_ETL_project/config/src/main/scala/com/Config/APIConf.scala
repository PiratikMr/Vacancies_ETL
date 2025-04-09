package com.Config

import com.typesafe.config.Config

import scala.jdk.CollectionConverters.asScalaSetConverter

class APIConf(conf: Config, path: String, apiHeader: String) extends Serializable {

  private val headersConfig: Config = conf.getConfig(f"$path$apiHeader")

  val headers: Map[String, String] = headersConfig.entrySet().asScala.map { entry =>
    val key = entry.getKey
    val value = headersConfig.getString(key)
    key -> value
  }.toMap
}
