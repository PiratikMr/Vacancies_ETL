package org.example.adzuna.config

import org.example.config.Loaders.ArgsLoader
import org.example.config.Loaders.modules.WithCommonArgsConfig

class AdzunaArgsLoader(args: Array[String])
  extends ArgsLoader(args)
    with WithCommonArgsConfig
{
  verify()
}
