package org.example.core.normalization.engine

import org.apache.spark.sql.{Dataset, SparkSession}
import org.example.core.normalization.engine.model.{FuzzyCandidate, FuzzyDictionary, FuzzyMatch}
import org.example.core.normalization.engine.similarity.SimilarityStrategy

class BroadcastTagExtractor(
                             spark: SparkSession,
                             similarityStrategy: SimilarityStrategy
                           ) extends Serializable {

  import spark.implicits._

  def extractExactTags(
                        candidatesDs: Dataset[FuzzyCandidate],
                        dictionaryDs: Dataset[FuzzyDictionary]
                      ): Dataset[FuzzyMatch] = {

    val dictRows = dictionaryDs.select("id", "normValue").collect()

    if (dictRows.isEmpty) {
      return spark.emptyDataset[FuzzyMatch]
    }

    val dictMap = dictRows
      .groupBy(_.getAs[String]("normValue"))
      .map { case (normVal, rows) =>
        normVal -> rows.map(_.getAs[Long]("id"))
      }

    val maxN = if (dictMap.isEmpty) 1 else dictMap.keys.map(_.split("\\s").length).max

    val dictBCast = spark.sparkContext.broadcast(dictMap)
    val maxNBCast = spark.sparkContext.broadcast(maxN)


    val candidatesWithNorm = candidatesDs
      .withColumn("normValue", similarityStrategy.normalize($"rawValue"))
      .select("entityId", "normValue")
      .as[(String, String)]


    val matchedDs = candidatesWithNorm.mapPartitions { iter =>
      val localDict = dictBCast.value
      val localMaxN = maxNBCast.value

      iter.flatMap { case (entityId, normValue) =>
        if (normValue == null || normValue.trim.isEmpty) {
          Seq.empty[FuzzyMatch]
        } else {
          val tokens = normValue.trim.split("\\s+")
          val limitN = math.min(localMaxN, tokens.length)

          val matchedDictIds = (1 to limitN).flatMap { n =>
            tokens.sliding(n).flatMap { chunk =>
              val sortedChunkKey = chunk.sorted.mkString(" ")
              localDict.getOrElse(sortedChunkKey, Array.empty[Long])
            }
          }.toSet

          matchedDictIds.map(dictId => FuzzyMatch(entityId, dictId))
        }
      }
    }

    matchedDs.distinct()
  }

}
