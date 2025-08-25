package com.files

import com.typesafe.config.Config
import scala.jdk.CollectionConverters.asScalaBufferConverter

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

  implicit object IntSeqGetter extends ConfigGetter[Seq[Int]] {
    def get(config: Config, path: String): Seq[Int] = config.getIntList(path).asScala.map(_.toInt)
  }
}
