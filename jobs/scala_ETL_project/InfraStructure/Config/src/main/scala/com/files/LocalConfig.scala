package com.files

import com.files.Common.{DBConf, FSConf, SparkConf, URLConf}
import com.typesafe.config.{Config, ConfigFactory}
import org.rogach.scallop.{ScallopConf, ScallopOption}

import java.io.File
import java.nio.file.Paths
import scala.jdk.CollectionConverters.asScalaSetConverter

abstract class LocalConfig (args: Seq[String]) extends ScallopConf(args) with Serializable {

  private val confFile: ScallopOption[String] = opt[String](name = "conffile")
  private lazy val conf: Config = ConfigFactory.parseFile(new File(confFile())).resolve()
  private lazy val site: String =  Paths.get(confFile()).getFileName.toString.stripSuffix(".conf")
  val currDate: ScallopOption[String] = opt[String](name = "filename", default = Some("undefined"))

  final def getFromConfFile[T](field: String)(implicit configGetter: ConfigGetter[T]): T = {
    configGetter.get(conf, s"Arguments.$field")
  }

  final def define(): Unit = {
    verify()
    conf
  }

  private lazy val dbconfig: Config = conf.getConfig("DB")
  lazy val dbConf: DBConf = DBConf(
    name = dbconfig.getString("userName"),
    pass = dbconfig.getString("userPassword"),
    host = dbconfig.getString("host"),
    port = dbconfig.getString("port"),
    base = dbconfig.getString("baseName"),
    platform = site
  )

  private lazy val fsconfig: Config = conf.getConfig("FS")
  lazy val fsConf: FSConf = FSConf(
    url = fsconfig.getString("url"),
    currDate = currDate(),
    rootPath = fsconfig.getString("path"),
    platform = site
  )

  private lazy val sparkconfig: Config = conf.getConfig("Spark")
  lazy val sparkConf: SparkConf = SparkConf(
    name = sparkconfig.getString("name"),
    master = sparkconfig.getString("master"),
    driverMemory = sparkconfig.getString("driverMemory"),
    driverCores = sparkconfig.getString("driverCores"),
    executorMemory = sparkconfig.getString("executorMemory"),
    executorCores = sparkconfig.getString("executorCores")
  )

  private lazy val urlconfig: Config = conf.getConfig("URL")
  private lazy val headersconf: Config = urlconfig.getConfig("headers")
  private lazy val urlheaders: Map[String, String] = headersconf.entrySet().asScala.map { entry =>
    val key = entry.getKey
    val value = headersconf.getString(key)
    key -> value
  }.toMap

  lazy val urlConf: URLConf = URLConf(
    headers = urlheaders,
    requestsPS = urlconfig.getInt("requestsPerSecond"),
    timeout = urlconfig.getInt("timeout")
  )
}
