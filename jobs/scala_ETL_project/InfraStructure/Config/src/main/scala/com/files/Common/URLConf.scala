package com.files.Common

case class URLConf(
                    headers: Map[String, String],
                    requestsPS: Int,
                    timeout: Int
                  ) extends Serializable
