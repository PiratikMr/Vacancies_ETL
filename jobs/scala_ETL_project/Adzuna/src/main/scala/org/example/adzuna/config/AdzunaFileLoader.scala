package org.example.adzuna.config

import org.example.config.Loaders.FileLoader
import org.example.config.Loaders.modules.{WithCommonFileConfig, WithStandardStructures}

class AdzunaFileLoader(confPath: String, currDate: String)
  extends FileLoader(confPath)
    with WithStandardStructures
    with WithCommonFileConfig
{
  override val saveFolder: String = currDate

  private val args = rootConfig.getConfig("Arguments")

  lazy val appId = args.getString("appId")

  lazy val appKey = args.getString("appKey")

  lazy val categoryTag = args.getString("categoryTag")

  lazy val maxDaysOld = args.getInt("maxDaysOld")
}
