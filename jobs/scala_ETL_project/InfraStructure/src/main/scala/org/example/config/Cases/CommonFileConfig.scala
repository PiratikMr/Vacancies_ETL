package org.example.config.Cases

case class CommonFileConfig(
                             rawPartitions: Int,
                             transformPartitions: Int,
                             updateLimit: Int,
                             apiBaseUrl: String
                           )
