package org.example.core.normalization.engine

import org.apache.spark.sql.expressions.Window.partitionBy
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.example.core.config.model.structures.FuzzyMatchSettings
import org.example.core.normalization.engine.FuzzyMatcher._
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

    val candidatesDf = candidatesDs.filter(col(C_RAW_VAL).isNotNull)
      .withColumn(C_NORM_VAL, similarityStrategy.normalize(col(C_RAW_VAL)))
      .alias("cand")
      .cache()

    val dictDf = dictionaryDs.toDF().alias("dict")


    // === 1. Точные совпадения ===
    val exactMatches = candidatesDf
      .join(
        dictDf,
        col(s"cand.$C_NORM_VAL") === col(s"dict.$D_NORM_VAL") &&
          col(s"cand.$C_PARENT_ID") === col(s"dict.$D_PARENT_ID")
      )
      .select(
        col(s"cand.$C_ENTITY_ID"),
        col(s"cand.$C_NORM_VAL"),
        col(s"dict.$D_ID").as("dictId")
      )


    val candidatesForFuzzy = candidatesDf
      .join(
        exactMatches,
        candidatesDf(C_ENTITY_ID) === exactMatches(C_ENTITY_ID) &&
          candidatesDf(C_NORM_VAL) === exactMatches(C_NORM_VAL),
        "left_anti"
      )
      .cache()

    if (candidatesForFuzzy.isEmpty) {
      candidatesDf.unpersist(blocking = false)
      candidatesForFuzzy.unpersist(blocking = false)
      return emptyResult(exactMatches)
    }
    // === Точные совпадения ===


    // === 2. Векторизация ===
    val fuzzyCandidatesWithFeatures = candidatesForFuzzy
      .withColumn(C_NORM_STRUCT, similarityStrategy.buildFeatures(col(C_NORM_VAL)))
      .cache()

    val dictWithFeatures = dictDf
      .withColumn("dict_norm_struct", similarityStrategy.buildFeatures(col(D_NORM_VAL)))
    // === Векторизация ===


    // === 3. Fuzzy matching со словарями ===
    val fuzzyDictMatches = matchDictionary(fuzzyCandidatesWithFeatures, dictWithFeatures)
      .alias("dict_fuzzy")
      .cache()

    val allDictMatches = exactMatches.drop(C_NORM_VAL)
      .unionByName(fuzzyDictMatches.select(col(C_ENTITY_ID), col("dictId")))
      .distinct()

    val remainingCandidates = fuzzyCandidatesWithFeatures
      .join(
        fuzzyDictMatches,
        fuzzyCandidatesWithFeatures(C_ENTITY_ID) === fuzzyDictMatches(C_ENTITY_ID),
        "left_anti"
      ).cache()

    if (remainingCandidates.isEmpty) {
      candidatesDf.unpersist(blocking = false)
      candidatesForFuzzy.unpersist(blocking = false)
      fuzzyDictMatches.unpersist(blocking = false)
      remainingCandidates.unpersist(blocking = false)
      fuzzyCandidatesWithFeatures.unpersist(blocking = false)
      return emptyResult(allDictMatches)
    }
    // === Fuzzy matching со словарями ===


    // === 4. Self fuzzy matching ===
    val (rawSelfMatches, clearSelfMatches) = selfMatching(remainingCandidates)
    val selfMatches = rawSelfMatches.cache()

    val toCreate = selfMatches
      .select($"$C_ENTITY_ID", $"$C_RAW_VAL", $"$C_PARENT_ID")
      .distinct()
      .as[FuzzyToCreate]

    val newMappingData = selfMatches
      .select($"$C_NORM_VAL", $"isCanonical", $"$C_RAW_VAL", $"$C_PARENT_ID")
      .distinct()
      .as[FuzzyMappingMeta]

    val matchedResult = allDictMatches.as[FuzzyMatch]

    val clearAllCaches = () => {
      candidatesDf.unpersist(blocking = false)
      candidatesForFuzzy.unpersist(blocking = false)
      fuzzyDictMatches.unpersist(blocking = false)
      remainingCandidates.unpersist(blocking = false)
      selfMatches.unpersist(blocking = false)
      fuzzyCandidatesWithFeatures.unpersist(blocking = false)
      clearSelfMatches()
    }
    // === Self fuzzy matching ===

    FuzzyMatcherResult(matchedResult, toCreate, newMappingData, clearAllCaches)
  }


  private def matchDictionary(candDf: DataFrame, dictDf: DataFrame): DataFrame = {
    val cand = candDf.alias("c")
    val dict = dictDf.alias("d")

    cand
      .join(dict, $"c.$C_PARENT_ID" === $"d.$D_PARENT_ID")
      .withColumn("score", similarityStrategy.calculateScore($"c.$C_NORM_STRUCT", $"d.dict_norm_struct"))
      .filter($"score" >= minScore)
      .withColumn("rank",
        row_number().over(
          partitionBy($"c.$C_NORM_VAL", $"c.$C_ENTITY_ID", $"c.$C_PARENT_ID")
            .orderBy($"score".desc, $"d.$D_NORM_VAL")
        )
      )
      .filter($"rank" === 1)
      .select(
        $"c.$C_ENTITY_ID",
        $"d.$D_ID".as("dictId")
      )
      .distinct()
  }

  private def selfMatching(candidatesDf: DataFrame): (DataFrame, () => Unit) = {
    val rawValue = C_RAW_VAL
    val normValue = C_NORM_VAL
    val normStruct = C_NORM_STRUCT
    val parentId = C_PARENT_ID
    val entityId = C_ENTITY_ID

    val aV = s"A_$rawValue"
    val aN = s"A_$normValue"
    val aS = s"A_$normStruct"
    val aPid = s"A_$parentId"

    val bId = s"B_$entityId"
    val bN = s"B_$normValue"
    val bS = s"B_$normStruct"
    val bPid = s"B_$parentId"

    val uniqueNorms = candidatesDf
      .select(normValue, normStruct, parentId)
      .distinct()
      .localCheckpoint()

    val uA = uniqueNorms.select(
      col(normValue).as(aN),
      col(normStruct).as(aS),
      col(parentId).as("A_pid")
    )

    val uB = uniqueNorms.select(
      col(normValue).as(bN),
      col(normStruct).as(bS),
      col(parentId).as("B_pid")
    )

    val lenA = length(col(aN))
    val lenB = length(col(bN))
    val lengthFilter = abs(lenA - lenB) <= (greatest(lenA, lenB) * lit(0.5))

    val halfPairs = uA
      .join(uB, col("A_pid") === col("B_pid") && col(aN) < col(bN) && lengthFilter)
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
      col(normValue).as(aN),
      col(normValue).as(bN),
      col(parentId).as(aPid),
      lit(1.0).as("score")
    )

    val allUniquePairs = halfPairs
      .unionByName(flippedPairs)
      .unionByName(selfPairs)
      .cache()

    val A = candidatesDf.select(
      col(rawValue).as(aV),
      col(normValue).as("u_aN"),
      col(parentId).as("u_aPid")
    )
    val B = candidatesDf.select(
      col(entityId).as(bId),
      col(normValue).as("u_bN"),
      col(parentId).as("u_bPid")
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
      ).cache()

    val activeHubs = rankedCandidates
      .filter(col("rank1") === 1)
      .filter(col(aN) =!= col(bN))
      .select(col(aN).as("hub"), col(aPid).as("hub_pid"))
      .distinct()

    val resultDf = rankedCandidates
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
      .withColumn("isCanonical", col(aN) === col(bN)) // Важно: имя колонки string literal
      .select(
        col(aV).as(rawValue),
        col(bN).as(normValue),
        col("isCanonical"),
        col(bId).as(entityId),
        col(aPid).as(parentId)
      ).distinct()

    val unpersistFn = () => {
      allUniquePairs.unpersist(blocking = false)
      rankedCandidates.unpersist(blocking = false)
      ()
    }

    (resultDf, unpersistFn)
  }

  private def emptyResult(matchedDf: DataFrame): FuzzyMatcherResult = {
    FuzzyMatcherResult(
      matched = matchedDf.as[FuzzyMatch],
      toCreate = spark.emptyDataset[FuzzyToCreate],
      mappingData = spark.emptyDataset[FuzzyMappingMeta],
      () => {}
    )
  }
}

object FuzzyMatcher {

  private val C_ENTITY_ID = "entityId"
  private val C_RAW_VAL = "rawValue"
  private val C_NORM_VAL = "normValue"
  private val C_PARENT_ID = "parentId"

  private val C_NORM_STRUCT = "normStruct"

  private val D_ID = "id"
  private val D_NORM_VAL = "normValue"
  private val D_PARENT_ID = "parentId"


  def apply(spark: SparkSession, settings: FuzzyMatchSettings): FuzzyMatcher = {
    new FuzzyMatcher(spark, new DefaultSimilarityStrategy(settings), settings.minScoreThreshold)
  }
}