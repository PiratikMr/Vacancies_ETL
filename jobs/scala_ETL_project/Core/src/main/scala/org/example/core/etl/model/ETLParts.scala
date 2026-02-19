package org.example.core.etl.model

import scala.util.{Failure, Success, Try}

sealed trait ETLPart

object ETLParts {

  case object Extract extends ETLPart
  case object TransformLoad extends ETLPart
  case object Update extends ETLPart

  def parse(str: String): Try[ETLPart] = {
    str.trim.toLowerCase match {
      case "e" => Success(Extract)
      case "tl" => Success(TransformLoad)
      case "u" => Success(Update)
      case _ => Failure(new IllegalArgumentException(s"Неизвестный ETL модуль: $str"))
    }
  }
}