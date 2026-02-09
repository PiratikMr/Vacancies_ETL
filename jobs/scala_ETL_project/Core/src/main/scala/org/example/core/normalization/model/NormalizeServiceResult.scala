package org.example.core.normalization.model

import org.apache.spark.sql.DataFrame

case class NormalizeServiceResult(
                                   mappedDf: DataFrame,
                                   mappedIdCol: String
                                 )
