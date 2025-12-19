package org.example.config.Loaders.objects

import com.typesafe.config.Config
import org.example.config.Cases.Structures.{DBConf, FSConf, NetworkConf, SparkConf}

import scala.jdk.CollectionConverters._

object StructuresFileParsing {

  def parseFSConf(config: Config, platform: String, saveFolder: String): FSConf =
    {
      FSConf(
        url = config.getString("url"),
        saveFolder = saveFolder,
        rootPath = config.getString("path"),
        platform = platform
      )
    }

  def parseDBConf(config: Config, platform: String): DBConf =
    {
      DBConf(
        name = config.getString("userName"),
        pass = config.getString("userPassword"),
        host = config.getString("host"),
        port = config.getString("port"),
        base = config.getString("baseName"),
        platform = platform
      )
    }

  def parseSparkConf(config: Config): SparkConf =
    {
      SparkConf(
        name = config.getString("name"),
        master = config.getString("master"),
        driverMemory = config.getString("driverMemory"),
        driverCores = config.getString("driverCores"),
        executorMemory = config.getString("executorMemory"),
        executorCores = config.getString("executorCores")
      )
    }

  def parseNetworkConf(config: Config): NetworkConf =
    {
      NetworkConf(
        headers = config.getConfig("headers").entrySet().asScala.map { entry =>
          val key = entry.getKey
          val value = config.getConfig("headers").getString(key)
          key -> value
        }.toArray,
        requestsPS = config.getInt("requestsPerSecond"),
        timeout = config.getInt("timeout")
      )
    }
}
