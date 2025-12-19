package org.example.config.Loaders.modules

import com.typesafe.config.Config
import org.example.config.Cases.CommonFileConfig
import org.example.config.Loaders.FileLoader

trait WithCommonFileConfig {

  self: FileLoader[_] =>

  lazy val common: CommonFileConfig = {
    val args: Config = rootConfig.getConfig("Arguments")

    val rawPartitions: Int = args.getInt("rawPartitions")
    val transformPartitions: Int = args.getInt("transformPartitions")
    val updateLimit: Int = args.getInt("updateLimit")

    val apiBaseUrl: String = args.getString("apiBaseUrl")

    CommonFileConfig(rawPartitions, transformPartitions, updateLimit, apiBaseUrl)
  }
}
