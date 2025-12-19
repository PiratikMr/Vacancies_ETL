package org.example.config.Cases.Structures

case class NetworkConf(
                    headers: Array[(String, String)],
                    requestsPS: Int,
                    timeout: Int
                  )
