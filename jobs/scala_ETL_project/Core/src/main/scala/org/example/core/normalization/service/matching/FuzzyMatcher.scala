package org.example.core.normalization.service.matching

import org.apache.spark.sql.expressions.Window.partitionBy
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.example.core.config.model.structures.FuzzyMatchSettings
import org.example.core.normalization.model.FuzzyMatcherResult
import org.example.core.normalization.service.matching.FuzzyMatcher.{isCanonical, returnResult}

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
               candidatesDf: DataFrame,
               dictDf: DataFrame,

               cEntityId: String,
               cRawValue: String,
               cNormValue: String,
               cParentId: String,

               dId: String,
               dNormValue: String,
               dParentId: String
             ): FuzzyMatcherResult = {

    // 1. Матчинг со словарем
    val dictMatches = matchDictionary(
      candidatesDf,
      dictDf,
      cEntityId, cNormValue, cParentId,
      dId, dNormValue, dParentId
    ).alias("dict").cache()

    val cand = candidatesDf.alias("cand")

    // 2. Исключаем тех, кто сматчился со словарем
    // dictMatches возвращает колонки с префиксом dm_, поэтому конфликт исключен
    val remainingCandidates = cand
      .join(
        dictMatches,
        cand(cEntityId) === dictMatches(cEntityId) &&
          cand(cNormValue) === dictMatches(cNormValue) &&
          cand(cParentId) === dictMatches(cParentId),
        "left_anti"
      ).cache()

    val dictMatchesToReturn = dictMatches.select(
      cEntityId,
      cNormValue,
      dId
    )

    if (remainingCandidates.isEmpty) {
      remainingCandidates.unpersist()
      return returnResult(dictMatchesToReturn, spark.emptyDataFrame, spark.emptyDataFrame)
    }

    // 3. Self-Matching с полной изоляцией имен колонок
    val selfMatches = selfMatching(
      remainingCandidates,
      cEntityId,
      cRawValue,
      cNormValue,
      cParentId
    ).cache()

    val toCreate = selfMatches
      .select(cEntityId, cRawValue, cParentId)
      .distinct()

    val newMappingData = selfMatches
      .select(cNormValue, isCanonical, cRawValue, cParentId)
      .distinct()

    returnResult(dictMatchesToReturn, toCreate, newMappingData)
  }


  private def matchDictionary(
                               candidatesDf: DataFrame,
                               dictDf: DataFrame,
                               candEntityId: String,
                               candNormVal: String,
                               candParentId: String,
                               dictId: String,
                               dictNormVal: String,
                               dictParentId: String
                             ): DataFrame = {

    val cand = candidatesDf.alias("c")
    val dict = dictDf.alias("d")

    val cNormCol = col(s"c.$candNormVal")
    val dNormCol = col(s"d.$dictNormVal")

    val cParentCol = col(s"c.$candParentId")
    val dParentCol = col(s"d.$dictParentId")

    val cEntityIdCol = col(s"c.$candEntityId")
    val dIdCol = col(s"d.$dictId")

    cand
      .join(dict, cParentCol === dParentCol)
      .withColumn("score", calculateScore(cNormCol, dNormCol, settings.numberPenalty))
      .filter(col("score") >= settings.score)
      .withColumn("rank",
        row_number().over(
          partitionBy(cNormCol, cEntityIdCol, cParentCol)
            .orderBy(col("score").desc, dNormCol)
        )
      )
      .filter(col("rank") === 1)
      .select(
        cEntityIdCol,
        dIdCol,
        cNormCol,
        cParentCol
      )
      .distinct()
  }

  private def selfMatching(
                            candidatesDf: DataFrame,
                            entityId: String,
                            rawValue: String,
                            normValue: String,
                            parentId: String
                          ): DataFrame = {

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
      .filter($"score" >= settings.score)
      .cache()

    val authority = pairs
      .dropDuplicates(aN, bN)
      .groupBy(col(aN).as("auth_norm"), col(aPid).as("auth_parent"))
      .agg(sum("score").as("auth_score"))

    val rankedCandidates = pairs
      .join(
        authority,
        col(aN) === $"auth_norm" &&
          col(aPid) === $"auth_parent"
      )
      .withColumn("rank1",
        row_number().over(
          partitionBy(bN, aPid)
            .orderBy($"auth_score".desc, len(col(aV)).asc, col(aN).asc)
        )
      ).cache()

    val activeHubs = rankedCandidates
      .filter($"rank1" === 1)
      .filter(col(aN) =!= col(bN))
      .select(col(aN).as("hub"), col(aPid).as("hub_pid"))
      .distinct()

    rankedCandidates
      .join(
        activeHubs,
        $"hub" === col(bN) &&
          col(aN) =!= col(bN) &&
          col(aPid) === $"hub_pid",
        "left_anti"
      )
      .withColumn("rank2",
        row_number().over(
          partitionBy(bId, bN, aPid)
            .orderBy($"rank1".asc)
        )
      )
      .filter($"rank2" === 1)
      .withColumn(isCanonical, col(aN) === col(bN))
      .select(
        col(aV).as(rawValue),
        col(bN).as(normValue),
        col(isCanonical),
        col(bId).as(entityId),
        col(aPid).as(parentId)
      ).distinct()
  }

}

object FuzzyMatcher {

  private def returnResult(matchedDf: DataFrame, toCreateDf: DataFrame, mappingDataDf: DataFrame): FuzzyMatcherResult = {
    FuzzyMatcherResult(
      matchedDf = matchedDf,
      toCreateDf = toCreateDf,
      mappingDataDf = mappingDataDf,
      isCanonicalCol = isCanonical
    )
  }

  private val isCanonical = "is_canonical"
}