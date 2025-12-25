package org.example.currency.config

import org.example.config.Loaders.FileLoader
import org.example.config.Loaders.modules.{WithCommonFileConfig, WithStandardStructures}

class CurrencyFileLoader(confPath: String, currDate: String)
  extends FileLoader(confPath)
    with WithStandardStructures
    with WithCommonFileConfig
{
  override val saveFolder: String = currDate

  lazy val apiKey: String = rootConfig.getString("Arguments.apiKey")
}
