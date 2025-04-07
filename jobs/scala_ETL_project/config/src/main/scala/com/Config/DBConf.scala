package com.Config

import com.typesafe.config.Config

class DBConf(conf: Config, path: String) extends ConfFile(conf, path) {
  lazy val userName: String = getString("userName")
  lazy val userPassword: String = getString("userPassword")
  lazy val DBurl: String = {
    val host: String = getString("host")
    val port: String = getString("port")
    val baseName: String = getString("baseName")
    s"jdbc:postgresql://$host:$port/$baseName"
  }
}
