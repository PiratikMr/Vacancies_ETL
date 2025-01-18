package com.Config

object DBConfig extends LocalConfig {
  private lazy val host: String = getString("db.host")
  private lazy val port: String = getString("db.port")
  private lazy val baseName: String = getString("db.baseName")

  lazy val userName: String = getString("db.userName")
  lazy val userPassword: String = getString("db.userPassword")
  lazy val DBurl: String = s"jdbc:postgresql://$host:$port/$baseName"
}
