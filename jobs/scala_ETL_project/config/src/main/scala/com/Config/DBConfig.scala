package com.Config

object DBConfig extends LocalConfig {
  private lazy val host: String = getStringField("DB.host")
  private lazy val port: String = getStringField("DB.port")
  private lazy val baseName: String = getStringField("DB.baseName")

  lazy val userName: String = getStringField("DB.userName")
  lazy val userPassword: String = getStringField("DB.userPassword")
  lazy val DBurl: String = s"jdbc:postgresql://$host:$port/$baseName"
}
