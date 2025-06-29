package com.files

import com.typesafe.config.Config

abstract case class ConfPart(config: Config) extends Serializable
