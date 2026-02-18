package org.example.core.normalization.service.matching

import org.apache.spark.sql.expressions.Window.partitionBy
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.example.core.config.model.structures.FuzzyMatchSettings
import org.example.core.normalization.model._
import org.example.core.normalization.service.matching.FuzzyMatcher._

class FuzzyMatcher(
                    spark: SparkSession,
                    settings: FuzzyMatchSettings
                  ) {

  import SimilarityUtils._
  import spark.implicits._

  def normCol(rawCol: Column): Column = {
    array_join(
      array_sort(
        normalize(rawCol)
      ),
      " ")
  }

  def normArrayCol(rawCol: Column): Column = {
    normalize(rawCol)
  }

  def execute(
               candidatesDs: Dataset[FuzzyCandidate],
               dictionaryDs: Dataset[FuzzyDictionary]
             ): FuzzyMatcherResult = {

    val candidatesDf = candidatesDs
      .withColumn(C_NORM_VAL, normCol(col(C_RAW_VAL)))
      .alias("cand")
      .cache()

    val dictDf = dictionaryDs.toDF().alias("dict")


    // 2. Exact Match (Точное совпадение по нормализованному значению)
    // Ищем тех, у кого norm_value и parent_id полностью совпадают со словарем
    val exactMatches = candidatesDf
      .join(
        dictDf,
        col(s"cand.$C_NORM_VAL") === col(s"dict.$D_NORM_VAL") &&
          col(s"cand.$C_PARENT_ID") === col(s"dict.$D_PARENT_ID")
      )
      .select(
        col(s"cand.$C_ENTITY_ID"),
        col(s"cand.$C_NORM_VAL"),
        col(s"dict.$D_ID").as("dictId") // alias для контракта FuzzyMatchFound
      )

    // 3. Исключаем найденные точные совпадения из кандидатов
    // Используем left_anti join по ID сущности
    val candidatesForFuzzy = candidatesDf
      .join(
        exactMatches,
        candidatesDf(C_ENTITY_ID) === exactMatches(C_ENTITY_ID) &&
          candidatesDf(C_NORM_VAL) === exactMatches(C_NORM_VAL),
        "left_anti"
      )
      .cache()

    // Если кандидатов не осталось, возвращаем только точные совпадения
    if (candidatesForFuzzy.isEmpty) {
      candidatesDf.unpersist()
      return emptyResult(exactMatches)
    }


    // 1. Матчинг со словарем
    val fuzzyDictMatches = matchDictionary(candidatesForFuzzy, dictDf)
      .alias("dict_fuzzy")
      .cache()

    val allDictMatches = exactMatches.drop(C_NORM_VAL)
      .unionByName(fuzzyDictMatches.select(col(C_ENTITY_ID), col("dictId")))
      .distinct()

    // 2. Исключаем тех, кто сматчился со словарем
    val remainingCandidates = candidatesForFuzzy
      .join(
        fuzzyDictMatches,
        candidatesForFuzzy(C_ENTITY_ID) === fuzzyDictMatches(C_ENTITY_ID),
        "left_anti"
      ).cache()

    if (remainingCandidates.isEmpty) {
      remainingCandidates.unpersist()
      return emptyResult(allDictMatches)
    }


    // 3. Self-Matching с полной изоляцией имен колонок
    val selfMatches = selfMatching(remainingCandidates).cache()

    val toCreate = selfMatches
      .select(
        col(C_ENTITY_ID),
        col(C_RAW_VAL),
        col(C_PARENT_ID)
      )
      .distinct()
      .as[FuzzyToCreate]

    val newMappingData = selfMatches
      .select(
        col(C_NORM_VAL),
        col("isCanonical"), // Поле создается внутри selfMatching
        col(C_RAW_VAL),
        col(C_PARENT_ID)
      )
      .distinct()
      .as[FuzzyMappingMeta]

    val matchedResult = allDictMatches.as[FuzzyMatch]

    FuzzyMatcherResult(matchedResult, toCreate, newMappingData)
  }


  private def matchDictionary(candDf: DataFrame, dictDf: DataFrame): DataFrame = {
    val cand = candDf.alias("c")
    val dict = dictDf.alias("d")

    cand
      .join(dict, col(s"c.$C_PARENT_ID") === col(s"d.$D_PARENT_ID"))
      .withColumn("score", calculateScore(col(s"c.$C_NORM_VAL"), col(s"d.$D_NORM_VAL"), settings.numberPenalty))
      .filter(col("score") >= settings.score)
      .withColumn("rank",
        row_number().over(
          partitionBy(col(s"c.$C_NORM_VAL"), col(s"c.$C_ENTITY_ID"), col(s"c.$C_PARENT_ID"))
            .orderBy(col("score").desc, col(s"d.$D_NORM_VAL"))
        )
      )
      .filter(col("rank") === 1)
      .select(
        col(s"c.$C_ENTITY_ID"),
        col(s"d.$D_ID").as("dictId")
      )
      .distinct()
  }

  private def selfMatching(candidatesDf: DataFrame): DataFrame = {
    // Используем фиксированные имена из контракта
    val rawValue = C_RAW_VAL
    val normValue = C_NORM_VAL
    val parentId = C_PARENT_ID
    val entityId = C_ENTITY_ID

    val aV = s"A_$rawValue"
    val aN = s"A_$normValue"
    val aPid = s"A_$parentId"

    val bId = s"B_$entityId"
    val bN = s"B_$normValue"
    val bPid = s"B_$parentId"

    val A = candidatesDf.select(
      col(rawValue).as(aV),
      col(normValue).as(aN),
      col(parentId).as(aPid)
    )
    val B = candidatesDf.select(
      col(entityId).as(bId),
      col(normValue).as(bN),
      col(parentId).as(bPid)
    )

    val pairs = A
      .join(B, col(aPid) === col(bPid))
      .withColumn("score", calculateScore(col(aN), col(bN), settings.numberPenalty))
      .filter(col("score") >= settings.score)
      .cache()

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
      .withColumn("isCanonical", col(aN) === col(bN)) // Важно: имя колонки string literal
      .select(
        col(aV).as(rawValue),
        col(bN).as(normValue),
        col("isCanonical"),
        col(bId).as(entityId),
        col(aPid).as(parentId)
      ).distinct()
  }

  private def emptyResult(matchedDf: DataFrame): FuzzyMatcherResult = {
    FuzzyMatcherResult(
      matched = matchedDf.as[FuzzyMatch],
      toCreate = spark.emptyDataset[FuzzyToCreate],
      mappingData = spark.emptyDataset[FuzzyMappingMeta]
    )
  }
}

object FuzzyMatcher {

  private val C_ENTITY_ID = "entityId"
  private val C_RAW_VAL = "rawValue"
  private val C_NORM_VAL = "normValue" // Вычисляемое поле
  private val C_PARENT_ID = "parentId"

  private val D_ID = "id"
  private val D_NORM_VAL = "normValue"
  private val D_PARENT_ID = "parentId"

}