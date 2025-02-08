package com.Config

import com.typesafe.config.Config

abstract class filePath(conf: Config, path: String)  {
  protected final def getString(fieldName: String): String = {
    conf.getString(s"$path$fieldName")
  }
}
