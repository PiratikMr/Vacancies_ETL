package com.files

import com.files.FolderName.FolderName
import com.typesafe.config.{Config, ConfigFactory}
import org.rogach.scallop.{ScallopConf, ScallopOption}

import java.io.File
import java.nio.file.Paths

abstract class LocalConfig (args: Seq[String]) extends ScallopConf(args) with Serializable {

  private val confFile: ScallopOption[String] = opt[String](name = "conffile")
  private lazy val conf: Config = ConfigFactory.parseFile(new File(confFile())).resolve()
  private lazy val site: String =  Paths.get(confFile()).getFileName.toString.stripSuffix(".conf")
  val fileName: ScallopOption[String] = opt[String](name = "filename", default = Some("undefined"))

  final def getFromConfFile[T](field: String)(implicit configGetter: ConfigGetter[T]): T = {
    configGetter.get(conf, s"Arguments.$field")
  }

  final def define(): Unit = {
    verify()
    conf
  }


  final def tableName(folderName: FolderName): String = {
    val name: String = folderName.split("/").last.toLowerCase

    folderName match {
      case FolderName.Currency => name
      case _ => s"${site}_$name"
    }

  }

  lazy val commonConf: CommonConfig = new CommonConfig(conf, site, fileName())
}
