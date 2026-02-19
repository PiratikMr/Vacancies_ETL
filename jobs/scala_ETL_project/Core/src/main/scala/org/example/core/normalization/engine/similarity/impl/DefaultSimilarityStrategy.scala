package org.example.core.normalization.engine.similarity.impl

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{greatest, least, lit}
import org.example.core.config.model.structures.FuzzyMatchSettings
import org.example.core.normalization.engine.similarity.SimilarityStrategy

class DefaultSimilarityStrategy(settings: FuzzyMatchSettings) extends SimilarityStrategy {

  override def normalize(col: Column): Column =
    TextNormalizer.normalize(col)

  override def calculateScore(tokens1: Column, tokens2: Column): Column = {
    val levScore = ScoringAlgorithms.levenshteinScore(tokens1, tokens2)
    val ngramScore = ScoringAlgorithms.nGramScore(tokens1, tokens2, settings.ngramSize)

    val rawTextScore = (levScore * settings.levenshteinWeight) + (ngramScore * settings.ngramWeight)

    val numberFactor = ScoringAlgorithms.calculateNumberFactor(tokens1, tokens2)

    val scoreWithPenalty = rawTextScore + (rawTextScore * numberFactor * settings.numberPenalty)


    clamp(scoreWithPenalty)
  }

  private def clamp(score: Column): Column = {
    least(lit(1.0), greatest(lit(0.0), score))
  }
}
