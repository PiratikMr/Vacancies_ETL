package com.files

import com.files.Common.{DBConf, FSConf, SparkConf, URLConf}
import com.typesafe.config.{Config, ConfigFactory}
import org.rogach.scallop.{ScallopConf, ScallopOption}

import java.io.File
import java.nio.file.Paths
import scala.jdk.CollectionConverters.asScalaSetConverter

abstract class ProjectConfig(args: Array[String]) extends ScallopConf(args) {

  private val confFile: ScallopOption[String] = opt[String](name = "conffile")
  val currDate: ScallopOption[String] = opt[String](name = "filename", default = Some("undefined"))

  private lazy val config: Config = ConfigFactory.parseFile(new File(confFile())).resolve()
  private lazy val dbConfig: Config = config.getConfig("DB")
  private lazy val fsConfig: Config = config.getConfig("FS")
  private lazy val sparkConfig: Config = config.getConfig("Spark")
  private lazy val urlConfig: Config = config.getConfig("URL")


  final def getFromConfFile[T](field: String)(implicit configGetter: ConfigGetter[T]): T = {
    configGetter.get(config, s"Arguments.$field")
  }

  private lazy val site: String = Paths.get(confFile()).getFileName.toString.stripSuffix(".conf")


  lazy val dbConf: DBConf = DBConf(
    name = dbConfig.getString("userName"),
    pass = dbConfig.getString("userPassword"),
    host = dbConfig.getString("host"),
    port = dbConfig.getString("port"),
    base = dbConfig.getString("baseName"),
    platform = site
  )

  lazy val fsConf: FSConf = FSConf(
    url = fsConfig.getString("url"),
    currDate = currDate(),
    rootPath = fsConfig.getString("path"),
    platform = site
  )

  lazy val sparkConf: SparkConf = SparkConf(
    name = sparkConfig.getString("name"),
    master = sparkConfig.getString("master"),
    driverMemory = sparkConfig.getString("driverMemory"),
    driverCores = sparkConfig.getString("driverCores"),
    executorMemory = sparkConfig.getString("executorMemory"),
    executorCores = sparkConfig.getString("executorCores")
  )

  lazy val urlConf: URLConf = URLConf(
    headers = urlConfig.getConfig("headers").entrySet().asScala.map { entry =>
      val key = entry.getKey
      val value = urlConfig.getConfig("headers").getString(key)
      key -> value
    }.toArray,
    requestsPS = urlConfig.getInt("requestsPerSecond"),
    maxConcurrentStreams = urlConfig.getInt("maxConcurrentStreams"),
    timeout = urlConfig.getInt("timeout")
  )
}

