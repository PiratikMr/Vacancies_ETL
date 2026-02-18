package org.example.core.config.model.structures

case class FuzzyMatchSettings(
                               minScoreThreshold: Double,
                               numberPenalty: Double,

                               ngramWeight: Double = 0.5,
                               levenshteinWeight: Double = 0.5,
                               ngramSize: Int = 3
                             )
