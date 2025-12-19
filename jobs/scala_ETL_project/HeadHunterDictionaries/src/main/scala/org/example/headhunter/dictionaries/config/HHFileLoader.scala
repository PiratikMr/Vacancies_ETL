package org.example.headhunter.dictionaries.config

import org.example.config.Loaders.FileLoader
import org.example.config.Loaders.modules.{WithCommonFileConfig, WithStandardStructures}

class HHFileLoader(confPath: String, currDate: String)
  extends FileLoader(confPath)
    with WithStandardStructures
    with WithCommonFileConfig
{
  override val saveFolder: String = currDate
}
