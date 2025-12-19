package org.example.headhunter.config

import org.example.config.Loaders.ArgsLoader
import org.example.config.Loaders.modules.WithCommonArgsConfig
import org.rogach.scallop.ScallopOption

class HHArgsLoader(args: Array[String])
  extends ArgsLoader(args)
    with WithCommonArgsConfig
{
  private val dateFromOption: ScallopOption[String] = opt[String](name = "datefrom")
  lazy val dateFrom: String = dateFromOption()

  verify()
}
