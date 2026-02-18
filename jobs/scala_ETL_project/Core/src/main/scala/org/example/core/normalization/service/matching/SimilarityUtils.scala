package org.example.core.normalization.service.matching

import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object SimilarityUtils {

  def calculateScore(v1: Column, v2: Column, numberPenalty: Double, nGramSize: Int = 3): Column = {

    val textScore = calculateTextScore(v1, v2, lit(nGramSize))

    val numberFactor = calculateNumberFactor(v1, v2)

    textScore + (textScore * numberFactor * lit(numberPenalty))
  }

  def normalize(column: Column): Column = {
    val operations: Seq[Column => Column] = Seq(
      c => coalesce(c, lit("")),
      c => lower(c),
      c => translate(c, "ёй", "еи"),
      c => regexp_replace(c, "[^a-z0-9а-я+#]", " "),
      c => regexp_replace(c, "\\s+", " "),
      c => trim(c)
    )

    val normalized = operations.foldLeft(column)((res, op) => op(res))

    val words = split(normalized, " ")

    val nonEmptyWords = filter(words, w => length(w) > 0)

    transform(nonEmptyWords, w => {
      concat(lit("_"), stemmerUdf(w), lit("_"))
    })
  }


  private val charsNGramsUdf = udf((text: String, n: Int) => {
    if (text == null || text.length < n) Seq.empty[String]
    else text.sliding(n).toSeq
  })

  private val stemmerUdf: UserDefinedFunction = udf((word: String) => {
    StemmerProvider.stem(word)
  })


  private def extractNumbers(col: Column): Column = {
    val onlyDigits = regexp_replace(col, "[^0-9]+", " ")
    val splitted = split(trim(onlyDigits), "\\s+")

    array_remove(splitted, "")
  }

  private def calculateTextScore(v1: Column, v2: Column, nGramSize: Column): Column = {

    val g1 = array_distinct(charsNGramsUdf(v1, lit(nGramSize)))
    val g2 = array_distinct(charsNGramsUdf(v2, lit(nGramSize)))

    val len1 = size(g1)
    val len2 = size(g2)

    when((len1 + len2) === 0, lit(0.0))
      .otherwise({
        val intersection = size(array_intersect(g1, g2))
        lit(2.0) * intersection / (len1 + len2)
      })
  }

  private def calculateNumberFactor(v1: Column, v2: Column): Column = {

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

}
