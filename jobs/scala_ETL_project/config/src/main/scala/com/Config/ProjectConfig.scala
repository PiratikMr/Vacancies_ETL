package com.Config

import com.typesafe.config.{Config, ConfigFactory}

class ProjectConfig(fileName: String) {
  private val conf: Config = ConfigFactory.load(fileName)

  val api: APIConf = new APIConf(conf, "API.")
  val db: DBConf = new DBConf(conf, "DB.")
  val fs: FSConf = new FSConf(conf, "FS.")
  val spark: SparkConf = new SparkConf(conf, "Spark.")

  /*private trait filePath {
    protected val path: String
    protected final def getString(fieldName: String): String = {
      conf.getString(s"$path$fieldName")
    }
  }

  object API extends filePath {
    override val path: String = "API."

    private lazy val accessToken: String = getString("accessToken")
    private lazy val userAgent: String = getString("userAgent")

    lazy val headers: Map[String, String] = Map(
      "Authorization" -> s"Bearer $accessToken",
      "User-Agent" -> userAgent
    )
  }

  object DB extends filePath {
    override val path: String = "DB."

    lazy val userName: String = getString("userName")
    lazy val userPassword: String = getString("userPassword")
    lazy val DBurl: String = {
      val host: String = getString("host")
      val port: String = getString("port")
      val baseName: String = getString("baseName")
      s"jdbc:postgresql://$host:$port/$baseName"
    }

  }

  object FS extends filePath {
    override val path: String = "FS."

    private lazy val fs: String = getString("url")
    private lazy val currentDate: String = LocalDate.now(ZoneId.of(getString("zoneId")))
      .format(DateTimeFormatter.ISO_LOCAL_DATE)

    lazy val vacanciesRawFileName: String = getString("fileName.vacanciesRaw")
    lazy val vacanciesTransformedFileName: String = getString("fileName.vacanciesTransformed")

    def getPath(isRoot: Boolean, fileName: String = ""): String = fs + (if (isRoot) s"$currentDate/") + fileName
  }

  object Spark extends filePath {
    override protected val path: String = "Spark."

    lazy val name: String = getString("name")
    lazy val master: String = getString("master")
  }*/
}
