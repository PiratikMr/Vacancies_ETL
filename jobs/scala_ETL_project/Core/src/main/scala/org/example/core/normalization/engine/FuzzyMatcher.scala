package org.example.core.normalization.engine

import org.apache.spark.sql.expressions.Window.partitionBy
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.example.core.config.model.structures.FuzzyMatchSettings
import org.example.core.normalization.engine.FuzzyMatcher._
import org.example.core.normalization.engine.model.FuzzyColumns._
import org.example.core.normalization.engine.model._
import org.example.core.normalization.engine.similarity.SimilarityStrategy
import org.example.core.normalization.engine.similarity.impl.DefaultSimilarityStrategy

class FuzzyMatcher(
                    spark: SparkSession,

                    similarityStrategy: SimilarityStrategy,
                    minScore: Double
                  ) {

  import spark.implicits._

  private val tagExtractor = new BroadcastTagExtractor(spark, similarityStrategy)


  def extractTags(
                   candidatesDs: Dataset[FuzzyCandidate],
                   dictionaryDs: Dataset[FuzzyDictionary]
                 ): Dataset[FuzzyMatch] = {
    tagExtractor.extractExactTags(candidatesDs, dictionaryDs)
  }

  def execute(
               candidatesDs: Dataset[FuzzyCandidate],
               dictionaryDs: Dataset[FuzzyDictionary]
             ): FuzzyMatcherResult = {

    val candidatesDf = candidatesDs.filter(col(RAW_VALUE).isNotNull)
      .withColumn(NORM_VALUE, similarityStrategy.normalize(col(RAW_VALUE)))
      .select(ENTITY_ID, RAW_VALUE, PARENT_ID, NORM_VALUE)
      .localCheckpoint()

    val dictDf = dictionaryDs.toDF()
      .select(DICT_ID, NORM_VALUE, PARENT_ID)


    // === 1. Точные совпадения ===
    val exactMatches = candidatesDf.join(dictDf, Seq(NORM_VALUE, PARENT_ID))
      .select(ENTITY_ID, NORM_VALUE, DICT_ID)

    val candidatesForFuzzy = candidatesDf.join(exactMatches, Seq(ENTITY_ID, NORM_VALUE), "left_anti")
      .select(ENTITY_ID, RAW_VALUE, PARENT_ID, NORM_VALUE)
      .localCheckpoint()

    if (candidatesForFuzzy.isEmpty) {
      return emptyResult(exactMatches)
    }
    // === Точные совпадения ===


    // === 2. Векторизация ===
    val fuzzyCandidatesWithFeatures = candidatesForFuzzy.withColumn(NORM_STRUCT, similarityStrategy.buildFeatures(col(NORM_VALUE)))
      .select(ENTITY_ID, RAW_VALUE, PARENT_ID, NORM_VALUE, NORM_STRUCT)
      .localCheckpoint()

    val dictWithFeatures = dictDf.withColumn(NORM_STRUCT, similarityStrategy.buildFeatures(col(NORM_VALUE)))
      .select(DICT_ID, NORM_VALUE, PARENT_ID, NORM_STRUCT)
    // === Векторизация ===


    // === 3. Fuzzy matching со словарями ===
    val fuzzyDictMatches = matchDictionary(fuzzyCandidatesWithFeatures, dictWithFeatures)
      .select(ENTITY_ID, DICT_ID)
      .localCheckpoint()

    val allDictMatches = exactMatches.select(ENTITY_ID, DICT_ID)
      .unionByName(fuzzyDictMatches)
      .distinct()

    val remainingCandidates = {
      fuzzyCandidatesWithFeatures.join(fuzzyDictMatches, Seq(ENTITY_ID), "left_anti")
        .select(ENTITY_ID, RAW_VALUE, PARENT_ID, NORM_VALUE, NORM_STRUCT)
        .localCheckpoint()
    }

    if (remainingCandidates.isEmpty) {
      return emptyResult(allDictMatches)
    }
    // === Fuzzy matching со словарями ===


    // === 4. Self fuzzy matching ===
    val selfMatches = selfMatching(remainingCandidates)
      .select(RAW_VALUE, NORM_VALUE, IS_CANONICAL, ENTITY_ID, PARENT_ID)
      .localCheckpoint()


    val toCreate = selfMatches.select(ENTITY_ID, RAW_VALUE, PARENT_ID)
      .distinct()
      .as[FuzzyToCreate]

    val newMappingData = selfMatches.select(NORM_VALUE, IS_CANONICAL, RAW_VALUE, PARENT_ID)
      .distinct()
      .as[FuzzyMappingMeta]

    val matchedResult = allDictMatches.as[FuzzyMatch]
    // === Self fuzzy matching ===

    FuzzyMatcherResult(matchedResult, toCreate, newMappingData, () => {})
  }


  private def matchDictionary(candidates: DataFrame, dictionaries: DataFrame): DataFrame = {
    candidates.alias("c").join(dictionaries.alias("d"),
        $"c.$PARENT_ID" === $"d.$PARENT_ID" &&
          arrays_overlap($"c.$NORM_STRUCT.ngrams", $"d.$NORM_STRUCT.ngrams")
      )
      .withColumn("score", similarityStrategy.calculateScore($"c.$NORM_STRUCT", $"d.$NORM_STRUCT"))
      .filter($"score" >= minScore)
      .withColumn("rank",
        row_number().over(
          partitionBy($"c.$NORM_VALUE", $"c.$ENTITY_ID", $"c.$PARENT_ID")
            .orderBy($"score".desc, $"d.$NORM_VALUE")
        )
      )
      .filter($"rank" === 1)
      .select(
        $"c.$ENTITY_ID",
        $"d.$DICT_ID"
      )
      .distinct()
  }

  private def selfMatching(candidatesDf: DataFrame): DataFrame = {
    val rawValue = RAW_VALUE
    val normValue = NORM_VALUE
    val normStruct = NORM_STRUCT
    val parentId = PARENT_ID
    val entityId = ENTITY_ID

    val aV = s"A_$rawValue"
    val aN = s"A_$normValue"
    val aS = s"A_$normStruct"
    val aPid = s"A_$parentId"

    val bId = s"B_$entityId"
    val bN = s"B_$normValue"
    val bS = s"B_$normStruct"
    val bPid = s"B_$parentId"

    val uniqueNorms = candidatesDf
      .select(NORM_VALUE, NORM_STRUCT, PARENT_ID)
      .distinct()
      .localCheckpoint()

    val uA = uniqueNorms.select(
      col(NORM_VALUE).as(aN),
      col(NORM_STRUCT).as(aS),
      col(PARENT_ID).as("A_pid")
    )

    val uB = uniqueNorms.select(
      col(NORM_VALUE).as(bN),
      col(NORM_STRUCT).as(bS),
      col(PARENT_ID).as("B_pid")
    )

    val lenA = length(col(aN))
    val lenB = length(col(bN))
    val lengthFilter = abs(lenA - lenB) <= (greatest(lenA, lenB) * lit(0.5))

    val halfPairs = uA
      .join(uB,
        col("A_pid") === col("B_pid") &&
          col(aN) < col(bN) &&
          lengthFilter &&
          arrays_overlap($"$aS.ngrams", $"$bS.ngrams")
      )
      .withColumn("score", similarityStrategy.calculateScore(col(aS), col(bS)))
      .filter($"score" >= minScore)
      .select(col(aN), col(bN), $"A_pid".as(aPid), $"score")

    val flippedPairs = halfPairs.select(
      col(bN).as(aN),
      col(aN).as(bN),
      col(aPid),
      $"score"
    )

    val selfPairs = uniqueNorms.select(
      col(NORM_VALUE).as(aN),
      col(NORM_VALUE).as(bN),
      col(PARENT_ID).as(aPid),
      lit(1.0).as("score")
    )

    val allUniquePairs = halfPairs
      .unionByName(flippedPairs)
      .unionByName(selfPairs)
      .cache()

    val A = candidatesDf.select(
      col(RAW_VALUE).as(aV),
      col(NORM_VALUE).as("u_aN"),
      col(PARENT_ID).as("u_aPid")
    )
    val B = candidatesDf.select(
      col(ENTITY_ID).as(bId),
      col(NORM_VALUE).as("u_bN"),
      col(PARENT_ID).as("u_bPid")
    )

    val pairs = allUniquePairs
      .join(A, col(aN) === col("u_aN") && col(aPid) === col("u_aPid"))
      .join(B, col(bN) === col("u_bN") && col(aPid) === col("u_bPid"))
      .select(
        col(aV), col(aN), col(aPid),
        col(bId), col(bN), col("u_bPid").as(bPid),
        col("score")
      )
      .localCheckpoint()

    val authority = pairs
      .dropDuplicates(aN, bN)
      .groupBy(col(aN).as("auth_norm"), col(aPid).as("auth_parent"))
      .agg(sum("score").as("auth_score"))

    val rankedCandidates = pairs
      .join(
        authority,
        col(aN) === col("auth_norm") &&
          col(aPid) === col("auth_parent")
      )
      .withColumn("rank1",
        row_number().over(
          partitionBy(bN, aPid)
            .orderBy(col("auth_score").desc, length(col(aV)).asc, col(aN).asc)
        )
      )
      .localCheckpoint()

    val activeHubs = rankedCandidates
      .filter(col("rank1") === 1)
      .filter(col(aN) =!= col(bN))
      .select(col(aN).as("hub"), col(aPid).as("hub_pid"))
      .distinct()

    rankedCandidates
      .join(
        activeHubs,
        col("hub") === col(bN) &&
          col(aN) =!= col(bN) &&
          col(aPid) === col("hub_pid"),
        "left_anti"
      )
      .withColumn("rank2",
        row_number().over(
          partitionBy(bId, bN, aPid)
            .orderBy(col("rank1").asc)
        )
      )
      .filter(col("rank2") === 1)
      .withColumn(IS_CANONICAL, col(aN) === col(bN))
      .select(
        col(aV).as(RAW_VALUE),
        col(bN).as(NORM_VALUE),
        col(IS_CANONICAL),
        col(bId).as(ENTITY_ID),
        col(aPid).as(PARENT_ID)
      ).distinct()
  }

  private def emptyResult(matchedDf: DataFrame, cache: () => Unit = () => {}): FuzzyMatcherResult = {
    FuzzyMatcherResult(
      matched = matchedDf.as[FuzzyMatch],
      toCreate = spark.emptyDataset[FuzzyToCreate],
      mappingData = spark.emptyDataset[FuzzyMappingMeta],
      cache
    )
  }
}

object FuzzyMatcher {
  private val NORM_STRUCT = "normStruct"

  def apply(spark: SparkSession, settings: FuzzyMatchSettings): FuzzyMatcher = {
    new FuzzyMatcher(spark, new DefaultSimilarityStrategy(settings), settings.minScoreThreshold)
  }
}