package com.Config

object APIConfig extends LocalConfig {
  lazy val accessToken: String = getStringField("API.accessToken")
  lazy val userAgent: String = getStringField("API.userAgent")
}
