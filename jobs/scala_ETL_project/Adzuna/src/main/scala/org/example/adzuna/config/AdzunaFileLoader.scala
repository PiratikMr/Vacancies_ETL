package org.example.adzuna.config

import org.example.config.Loaders.FileLoader
import org.example.config.Loaders.modules.{WithCommonFileConfig, WithStandardStructures}

class AdzunaFileLoader(confPath: String, currDate: String, locationIndex: Int)
  extends FileLoader(confPath)
    with WithStandardStructures
    with WithCommonFileConfig
{
  override val saveFolder: String = currDate

  private val args = rootConfig.getConfig("Arguments")

  lazy val apiParams = AdzunaApiParams(
    args.getStringList("locationTags").get(locationIndex),
    args.getInt("maxDaysOld"),
    args.getInt("vacsPerPage"),
    args.getString("appId"),
    args.getString("appKey"),
    args.getString("categoryTag")
  )

  lazy val currency: String = args.getStringList("currencies").get(locationIndex)
  lazy val urlDomain: String = args.getStringList("urlDomains").get(locationIndex)

  lazy val pageLimit: Int = args.getInt("pageLimit")
}
