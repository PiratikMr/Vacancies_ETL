package org.example.core.normalization.engine.similarity

import org.apache.spark.sql.Column

trait SimilarityStrategy {

  def normalize(col: Column): Column

  def calculateScore(c1: Column, c2: Column): Column

}
