package com.Config

import com.Config.FolderName.FolderName
import com.typesafe.config.{Config, ConfigFactory}

import scala.jdk.CollectionConverters.asScalaSetConverter

class ProjectConfig(genConfFile: String, apiHeader: String, date: String) {
  private val conf: Config = ConfigFactory.load(genConfFile)

  lazy val api = new ConfPart(conf.getConfig(f"API.$apiHeader")) {
    val headers: Map[String, String] = config.entrySet().asScala.map { entry =>
      val key = entry.getKey
      val value = config.getString(key)
      key -> value
    }.toMap
  }

  lazy val db = new ConfPart(conf.getConfig("DB")) {
    lazy val userName: String = config.getString("userName")
    lazy val userPassword: String = config.getString("userPassword")
    lazy val DBurl: String = {
      val host: String = config.getString("host")
      val port: String = config.getString("port")
      val baseName: String = config.getString("baseName")
      s"jdbc:postgresql://$host:$port/$baseName"
    }
  }

  lazy val fs = new ConfPart(conf.getConfig("FS")) {
    private lazy val url: String = config.getString("url")
/*    private lazy val currentDate: String = if (date == null) LocalDate.now(ZoneId.of(config.getString("zoneId")))
      .format(DateTimeFormatter.ISO_LOCAL_DATE) else date*/

    def getPath(file: FolderName, fileName: String): String = {
      if (file == FolderName.Dict) {
        f"$url$file/$fileName"
      } else {
        f"$url$file/$date"
      }
    }
  }

  lazy val spark = new ConfPart(conf.getConfig("Spark")) {
    lazy val name: String = config.getString("name")
    lazy val master: String = config.getString("master")
  }
}
