package com.Config

import com.typesafe.config.Config

abstract case class ConfPart(config: Config) extends Serializable {}
