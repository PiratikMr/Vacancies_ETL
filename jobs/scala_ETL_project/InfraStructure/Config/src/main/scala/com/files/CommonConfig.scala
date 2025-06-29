package com.files

import com.files.FolderName.FolderName
import com.typesafe.config.Config

import scala.jdk.CollectionConverters.asScalaSetConverter

class CommonConfig(config: Config, site: String, fileName: String) extends Serializable {
  private lazy val headersConf: Config = config.getConfig(s"Arguments.headers")
  lazy val headers: Map[String, String] = headersConf.entrySet().asScala.map { entry =>
    val key = entry.getKey
    val value = headersConf.getString(key)
    key -> value
  }.toMap

  lazy val db = new ConfPart(config.getConfig("DB")) {
    lazy val userName: String = config.getString("userName")
    lazy val userPassword: String = config.getString("userPassword")
    lazy val DBurl: String = {
      val host: String = config.getString("host")
      val port: String = config.getString("port")
      val baseName: String = config.getString("baseName")
      s"jdbc:postgresql://$host:$port/$baseName"
    }
  }

  lazy val fs = new ConfPart(config.getConfig("FS")) {
    lazy val url: String = config.getString("url")
    private lazy val mainPath: String = config.getString("path")

    def getPath(folderName: FolderName): String = {
      s"$url/$mainPath$site/$folderName${
        if (FolderName.isDict(folderName)) "" else s"/$fileName"
      }"
    }
  }

  lazy val spark = new ConfPart(config.getConfig("Spark")) {
    lazy val name: String = config.getString("name")
    lazy val master: String = config.getString("master")
  }
}
