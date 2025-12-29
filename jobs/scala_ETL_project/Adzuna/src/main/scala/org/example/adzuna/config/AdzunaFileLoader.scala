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

  lazy val apiParams = AdzunaApiParams(
    args.getString("locationTag"),
    args.getInt("maxDaysOld"),
    args.getInt("vacsPerPage"),
    args.getString("appId"),
    args.getString("appKey"),
    args.getString("categoryTag")
  )

  lazy val pageLimit = args.getInt("pageLimit")
}
