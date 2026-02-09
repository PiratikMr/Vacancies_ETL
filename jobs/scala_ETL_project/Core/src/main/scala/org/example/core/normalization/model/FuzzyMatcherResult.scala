package org.example.core.normalization.model

import org.apache.spark.sql.DataFrame

case class FuzzyMatcherResult(
                             matchedDf: DataFrame,
                             toCreateDf: DataFrame,
                             mappingDataDf: DataFrame,

                             isCanonicalCol: String
                             )
