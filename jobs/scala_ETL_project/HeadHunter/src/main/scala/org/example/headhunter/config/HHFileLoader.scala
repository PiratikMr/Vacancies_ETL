package org.example.headhunter.config

import org.example.config.Loaders.FileLoader
import org.example.config.Loaders.modules.{WithCommonFileConfig, WithStandardStructures}

import scala.jdk.CollectionConverters.CollectionHasAsScala

class HHFileLoader(confPath: String, currDate: String)
  extends FileLoader(confPath)
    with WithStandardStructures
    with WithCommonFileConfig
{
  override val saveFolder: String = currDate

  private val args = rootConfig.getConfig("Arguments")

  lazy val fieldIDs: Seq[Int] = args.getIntList("fieldIds").asScala.map(_.toInt).toSeq
  lazy val vacsPerPage: Int = args.getInt("vacsPerPage")
  lazy val pageLimit: Int = args.getInt("pageLimit")
}
