package org.example.config.Cases.Structures

case class SparkConf(
               name: String,
               master: String,
               driverMemory: String,
               driverCores: String,
               executorMemory: String,
               executorCores: String
               )