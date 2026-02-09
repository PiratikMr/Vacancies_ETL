package org.example.core.normalization.model

case class MappingDimTableConf(
                                tableName: String,
                                idColName: String,
                                mappedValueColName: String,
                                isOrigin: String
                              )
