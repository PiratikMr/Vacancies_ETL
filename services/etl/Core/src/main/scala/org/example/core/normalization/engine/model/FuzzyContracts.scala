package org.example.core.normalization.engine.model

case class FuzzyCandidate(
                           entityId: String,
                           rawValue: String,
                           parentId: Long
                         )

case class FuzzyDictionary(
                            dictId: Long,
                            mappingId: Long,
                            normValue: String,
                            parentId: Long
                          )

case class FuzzyMatch(
                       entityId: String,
                       dictId: Long,
                       mappingId: Long,
                       rawValue: String,
                       score: Double
                     )

case class FuzzyToCreate(
                          entityId: String,
                          hubValue: String,
                          rawValue: String,
                          normValue: String,
                          parentId: Long,
                          score: Double
                        )

case class FuzzyMappingMeta(
                             hubValue: String,
                             normValue: String,
                             isCanonical: Boolean,
                             parentId: Long,
                             linkScore: Double
                           )

object FuzzyColumns {
  val ENTITY_ID = "entityId"
  val RAW_VALUE = "rawValue"
  val HUB_VALUE = "hubValue"
  val PARENT_ID = "parentId"
  val NORM_VALUE = "normValue"
  val DICT_ID = "dictId"
  val MAPPING_ID = "mappingId"
  val SCORE = "score"
  val LINK_SCORE = "linkScore"

  val IS_CANONICAL = "isCanonical"
}

object FuzzyScores {

  val EXACT = 1.0
}
