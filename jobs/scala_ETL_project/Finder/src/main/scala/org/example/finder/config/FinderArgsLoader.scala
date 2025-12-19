package org.example.finder.config

import org.example.config.Loaders.ArgsLoader
import org.example.config.Loaders.modules.WithCommonArgsConfig

class FinderArgsLoader(args: Array[String])
  extends ArgsLoader(args)
    with WithCommonArgsConfig
{
  verify()
}
