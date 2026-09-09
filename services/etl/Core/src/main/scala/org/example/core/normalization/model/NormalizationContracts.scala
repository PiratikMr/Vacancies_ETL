package org.example.core.normalization.model

import org.apache.spark.sql.{DataFrame, Dataset}
import org.example.core.config.database.MatchLogDef

object NormalizationColumns {
  val ENTITY_ID = "entityId"
  val RAW_VALUE = "rawValue"
  val MAPPED_ID = "mappedId"
  val MAPPING_ID = "mappingId"
  val SCORE = "score"
  val PARENT_ID = "parentId"
}

case class NormCandidate(
                          entityId: String,
                          rawValue: String,
                          parentId: Option[String]
                        )

case class NormMatch(
                      entityId: String,
                      mappedId: Long
                    )

case class MatchLogRow(
                        entityId: String,
                        mappingId: Long,
                        rawValue: String,
                        score: Double
                      )

case class NormalizeResult(
                            matches: Dataset[NormMatch],
                            log: Dataset[MatchLogRow]
                          )

case class MatchLogPart(
                         logDef: MatchLogDef,
                         rows: DataFrame
                       )

case class NormalizationOutput(
                                mappings: DataFrame,
                                matchLogs: Seq[MatchLogPart]
                              )
