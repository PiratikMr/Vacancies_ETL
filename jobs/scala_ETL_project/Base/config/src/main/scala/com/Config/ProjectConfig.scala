package com.Config

import com.Config.FolderName.FolderName
import com.typesafe.config.{Config, ConfigFactory}

import java.io.File
import scala.jdk.CollectionConverters.asScalaSetConverter

class ProjectConfig(genConfFile: String, site: String, date: String) {

  private val configFile: File = new File(genConfFile)
  if (!configFile.exists()) {
    throw new RuntimeException(s"Configuration file not found at: ${configFile.getAbsolutePath}")
  }

  private val conf: Config = ConfigFactory.parseFile(configFile)

  lazy val api = new ConfPart(conf.getConfig(f"API.$site")) {
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

    def getPath(folderName: FolderName): String = {
      s"$url$site/$folderName${
        if (!FolderName.isDict(folderName)) { s"/$date" } else ""
      }"
    }
  }

  lazy val spark = new ConfPart(conf.getConfig("Spark")) {
    lazy val name: String = config.getString("name")
    lazy val master: String = config.getString("master")
  }
}
