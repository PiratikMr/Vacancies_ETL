package org.example.core.normalization.engine.similarity.impl

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object ScoringAlgorithms {

  def levenshteinScore(v1: Column, v2: Column): Column = {
    val dist = levenshtein(v1, v2)
    val maxLen = greatest(length(v1), length(v2))

    when(maxLen === 0, lit(1.0))
      .otherwise(lit(1.0) - (dist / maxLen))
  }

  def nGramScore(v1: Column, v2: Column, n: Int): Column = {
    val g1 = array_distinct(charsNGramsUdf(v1, lit(n)))
    val g2 = array_distinct(charsNGramsUdf(v2, lit(n)))

    val len1 = size(g1)
    val len2 = size(g2)

    when((len1 + len2) === 0, lit(0.0))
      .otherwise({
        val intersection = size(array_intersect(g1, g2))
        lit(2.0) * intersection / (len1 + len2)
      })
  }

  def calculateNumberFactor(v1: Column, v2: Column): Column = {
    val n1 = extractNumbers(v1)
    val n2 = extractNumbers(v2)

    val s1 = size(n1)
    val s2 = size(n2)

    when((s1 + s2) === 0, lit(0.0))
      .otherwise({
        val intersection = size(array_intersect(n1, n2))
        val union = size(array_union(n1, n2))

        (lit(2.0) * intersection / union) - lit(1.0)
      })
  }


  private val charsNGramsUdf = udf((text: String, n: Int) => {
    if (text == null || text.length < n) Seq.empty[String]
    else text.sliding(n).toSeq
  })

  private def extractNumbers(c: Column): Column = {
    val onlyDigits = regexp_replace(c, "[^0-9]+", " ")
    val splitted = split(trim(onlyDigits), "\\s+")

    array_remove(splitted, "")
  }
}
