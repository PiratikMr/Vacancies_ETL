package org.example.core.normalization.config

case class DimTableConf(
                       tableName: String,
                       idColName: String,
                       valueColName: String,
                       parentIdColName: Option[String]
                       )


