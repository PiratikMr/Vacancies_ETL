package com.files.Common

case class URLConf(
                    headers: Array[(String, String)],
                    requestsPS: Int,
                    maxConcurrentStreams: Int,
                    timeout: Int
                  ) extends Serializable
