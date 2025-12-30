package org.example.adzuna.config

import org.example.config.Loaders.ArgsLoader
import org.example.config.Loaders.modules.WithCommonArgsConfig
import org.rogach.scallop.ScallopOption

class AdzunaArgsLoader(args: Array[String])
  extends ArgsLoader(args)
    with WithCommonArgsConfig
{
  val locationIndex: ScallopOption[Int] = opt[Int]("locidx", required = true)

  verify()
}
