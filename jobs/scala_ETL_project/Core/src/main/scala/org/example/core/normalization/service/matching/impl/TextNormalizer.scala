package org.example.core.normalization.service.matching.impl

import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object TextNormalizer {

  def normalize(c: Column): Column = {
    val cleaned = cleanOperations.foldLeft(c)((res, op) => op(res))

    val nonEmptyWords = filter(split(cleaned, " "), w => length(w) > 0)

    transform(nonEmptyWords, w => {
      concat(lit("_"), stemmerUdf(w), lit("_"))
    })
  }

  private val cleanOperations: Seq[Column => Column] = Seq(
    c => coalesce(c, lit("")),
    c => lower(c),
    c => translate(c, "ёй", "еи"),
    c => regexp_replace(c, "[^a-z0-9а-я+#]", " "),
    c => regexp_replace(c, "\\s+", " "),
    c => trim(c)
  )

  private val stemmerUdf: UserDefinedFunction = udf((word: String) => {
    StemmerProvider.stem(word)
  })
}
