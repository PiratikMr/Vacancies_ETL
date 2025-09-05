package com.files.Common

case class SparkConf(
               name: String,
               master: String,
               driverMemory: String,
               driverCores: String,
               executorMemory: String,
               executorCores: String
               ) extends Serializable
