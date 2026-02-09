package org.example.core.config.model

case class CommonFileConfig(
                             rawPartitions: Int,
                             transformPartitions: Int,
                             updateLimit: Int,
                             apiBaseUrl: String
                           )
