package org.example.core.normalization.service.matching

import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.tartarus.snowball.ext.{englishStemmer, russianStemmer}

object SimilarityUtils {

  def calculateScore(v1: Column, v2: Column, nGramSize: Int = 3): Column = {

    val g1 = charsNGramsUdf(v1, lit(nGramSize))
    val g2 = charsNGramsUdf(v2, lit(nGramSize))

    val intersection = size(array_intersect(g1, g2))

    val len1 = size(g1)
    val len2 = size(g2)

    when((len1 + len2) === 0, lit(0.0))
      .otherwise(
        lit(2.0) * intersection / (len1 + len2)
      )
  }

  def normalize(column: Column): Column = {
    val operations: Seq[Column => Column] = Seq(
      c => coalesce(c, lit("")),
      c => lower(c),
      c => regexp_replace(c, "[^a-z0-9а-я+#.-]", " "),
      c => regexp_replace(c, "\\s+", " "),
      c => translate(c, "ёй", "еи"),
      c => trim(c)
    )

    val normalized = operations.foldLeft(column)((res, op) => op(res))

    val words = split(normalized, " ")

    transform(words, w => {
      concat(lit("_"), stemmerUdf(w), lit("_"))
    })
  }



  private val charsNGramsUdf = udf((text: String, n: Int) => {
    if (text == null || text.length < n) Seq.empty[String]
    else text.sliding(n).toSeq
  })

  private val stemmerUdf: UserDefinedFunction = udf((word: String) => {
    if (word == null || word.isEmpty) ""
    else {
      val stemmer = {
        if (word.exists(c => c >= '\u0400' && c <= '\u04FF')) new russianStemmer()
        else new englishStemmer()
      }

      stemmer.setCurrent(word)
      stemmer.stem()
      stemmer.getCurrent
    }
  })


}
