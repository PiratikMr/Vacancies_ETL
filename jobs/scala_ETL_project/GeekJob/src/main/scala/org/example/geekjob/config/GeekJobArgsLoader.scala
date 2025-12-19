package org.example.geekjob.config

import org.example.config.Loaders.ArgsLoader
import org.example.config.Loaders.modules.WithCommonArgsConfig

class GeekJobArgsLoader(args: Array[String])
  extends ArgsLoader(args)
    with WithCommonArgsConfig
{
  verify()
}
