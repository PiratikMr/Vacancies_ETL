package org.example.config.Loaders

import org.rogach.scallop.ScallopConf

abstract class ArgsLoader(args: Array[String]) extends ScallopConf(args)