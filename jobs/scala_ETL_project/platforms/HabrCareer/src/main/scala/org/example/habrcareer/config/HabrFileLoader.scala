package org.example.habrcareer.config

import org.example.core.config.loader.FileLoader
import org.example.core.config.loader.module.{WithCommonFileConfig, WithStandardStructures}

class HabrFileLoader(confPath: String, currDate: String)
  extends FileLoader(confPath)
    with WithStandardStructures
    with WithCommonFileConfig
{
  override val saveFolder: String = currDate

  private lazy val args = rootConfig.getConfig("Arguments")

  lazy val vacsPageLimit: Int = math.max(1, args.getInt("vacsPageLimit"))
  lazy val vacsPerPage: Int = args.getInt("vacsPerPage")
}
