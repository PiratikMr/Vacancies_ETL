package org.example.core.normalization.engine.model

case class FuzzyCandidate(
                           entityId: String,
                           rawValue: String,
                           parentId: Long
                         )

case class FuzzyDictionary(
                            id: Long,
                            normValue: String,
                            parentId: Long
                          )

case class FuzzyMatch(
                       entityId: String,
                       dictId: Long
                     )

case class FuzzyToCreate(
                          entityId: String,
                          rawValue: String,
                          parentId: Long
                        )

case class FuzzyMappingMeta(
                             normValue: String,
                             isCanonical: Boolean,
                             rawValue: String,
                             parentId: Long
                           )

