package com.Config

import com.Config.FolderName.FolderName
import com.typesafe.config.{Config, ConfigFactory}
import org.rogach.scallop.{ScallopConf, ScallopOption}

import java.io.File
import java.nio.file.Paths

abstract class LocalConfig (args: Seq[String]) extends ScallopConf(args) {
  private val confFile: ScallopOption[String] = opt[String](name = "conffile")
  private lazy val conf: Config = ConfigFactory.parseFile(new File(confFile())).resolve()
  private lazy val site: String =  Paths.get(confFile()).getFileName.toString.stripSuffix(".conf")
  val fileName: ScallopOption[String] = opt[String](name = "filename", default = Some("undefined"))

  final def getFromConfFile[T](field: String)(implicit configGetter: ConfigGetter[T]): T = {
    implicitly[ConfigGetter[T]].get(conf, s"Arguments.$field")
  }

  final def define(): Unit = {
    verify()
    conf
  }


  final def tableName(folderName: FolderName): String = {
    s"${site}_$folderName"
  }

  lazy val commonConf: CommonConfig = new CommonConfig(conf, site, fileName())
}

//import com.Config.FolderName.FolderName
//import org.rogach.scallop.{ScallopConf, ScallopOption}
//
//abstract class LocalConfig (args: Seq[String], site: String) extends ScallopConf(args) {
//  private val fileName: ScallopOption[String] = opt[String](name = "fileName")
//  val date: ScallopOption[String] = opt[String](name = "date", default = Some(null))
//
//  lazy val fileConf: ProjectConfig = new ProjectConfig(fileName(), site, date())
//
//  def tableName(folderName: FolderName): String = {
//   s"${site}_$folderName"
//  }
//
//  def define(): Unit = {
//    verify()
//    fileConf
//  }
//}
