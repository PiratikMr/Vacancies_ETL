package org.example.core.config.loader.module

import org.example.core.config.loader.FileLoader
import org.example.core.config.loader.parsing.StructuresFileParsing
import org.example.core.config.model.structures.StructuresConfig

import java.nio.file.Paths

trait WithStandardStructures {
  self: FileLoader[_] =>

  val saveFolder: String

  lazy val structures: StructuresConfig = {
    val platform: String = Paths.get(confPath).getFileName.toString.stripSuffix(".conf")

    StructuresConfig(
      StructuresFileParsing.parseDBConf(rootConfig.getConfig("DataBase")),
      StructuresFileParsing.parseFSConf(rootConfig.getConfig("FileSystem"), platform, saveFolder),
      StructuresFileParsing.parseSparkConf(rootConfig.getConfig("Spark")),
      StructuresFileParsing.parseNetworkConf(rootConfig.getConfig("Network")),
      StructuresFileParsing.parseFuzzyMatcherConf(rootConfig.getConfig("FuzzyMatcher"))
    )
  }
}
