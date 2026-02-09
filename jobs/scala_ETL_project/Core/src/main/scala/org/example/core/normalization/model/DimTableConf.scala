package org.example.core.normalization.model

case class DimTableConf(
                       tableName: String,
                       idColName: String,
                       valueColName: String,
                       parentIdColName: Option[String]
                       )


