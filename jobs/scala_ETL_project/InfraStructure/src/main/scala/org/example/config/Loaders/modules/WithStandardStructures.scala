package org.example.config.Loaders.modules

import org.example.config.Cases.Structures.StructuresConfig
import org.example.config.Loaders.FileLoader
import org.example.config.Loaders.objects.StructuresFileParsing

import java.nio.file.Paths

trait WithStandardStructures {
  self: FileLoader[_] =>

  val saveFolder: String

  lazy val structures: StructuresConfig = {
    val platform: String = Paths.get(confPath).getFileName.toString.stripSuffix(".conf")

    StructuresConfig(
      StructuresFileParsing.parseDBConf(rootConfig.getConfig("DB"), platform),
      StructuresFileParsing.parseFSConf(rootConfig.getConfig("FS"), platform, saveFolder),
      StructuresFileParsing.parseSparkConf(rootConfig.getConfig("Spark")),
      StructuresFileParsing.parseNetworkConf(rootConfig.getConfig("Net"))
    )
  }
}
