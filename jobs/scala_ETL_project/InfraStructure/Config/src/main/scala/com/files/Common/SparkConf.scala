package com.files.Common

case class SparkConf(
               name: String,
               master: String,
               defaultParallelism: Int
               ) extends Serializable
