package com.files

import com.typesafe.config.Config

sealed trait ConfigGetter[T] {
  def get(config: Config, path: String): T
}

object ConfigGetter {
  implicit object StringGetter extends ConfigGetter[String] {
    def get(config: Config, path: String): String = config.getString(path)
  }

  implicit object IntGetter extends ConfigGetter[Int] {
    def get(config: Config, path: String): Int = config.getInt(path)
  }
}
