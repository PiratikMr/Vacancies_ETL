package org.example.getmatch.config

import org.example.config.Loaders.ArgsLoader
import org.example.config.Loaders.modules.WithCommonArgsConfig

class GetMatchArgsLoader(args: Array[String]) extends ArgsLoader(args)
  with WithCommonArgsConfig {
  verify()
}