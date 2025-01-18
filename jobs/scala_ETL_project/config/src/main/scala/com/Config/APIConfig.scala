package com.Config

object APIConfig extends LocalConfig {
  lazy val accessToken: String = getString("hhapi.accessToken")
  lazy val userAgent: String = getString("hhapi.userAgent")
}
