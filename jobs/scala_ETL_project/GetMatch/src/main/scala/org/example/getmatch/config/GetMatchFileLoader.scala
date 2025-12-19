package org.example.getmatch.config

import org.example.config.Loaders.FileLoader
import org.example.config.Loaders.modules.{WithCommonFileConfig, WithStandardStructures}

class GetMatchFileLoader(confPath: String, currDate: String)
  extends FileLoader(confPath)
    with WithStandardStructures
    with WithCommonFileConfig
{
  override val saveFolder: String = currDate

  lazy val getMatch: GetMatchFileConfig = {
    val args = rootConfig.getConfig("Arguments")

    val vacsLimit = args.getInt("vacsLimit")
    val vacsPerPage = args.getInt("vacsPerPage")
    val inDays = args.getString("inDays")

    GetMatchFileConfig(vacsLimit, vacsPerPage, inDays)
  }
}