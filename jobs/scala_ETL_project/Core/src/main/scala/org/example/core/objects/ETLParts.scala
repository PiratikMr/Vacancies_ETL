package org.example.core.objects

object ETLParts extends Enumeration {
  type ETLPart = Value

  val EXTRACT, TRANSFORM_LOAD, UPDATE, UNRECOGNIZED = Value

  def parseString(str: String): ETLPart =
    str.trim.toLowerCase match {
      case "e" => EXTRACT
      case "tl" => TRANSFORM_LOAD
      case "u" => UPDATE
      case _ => UNRECOGNIZED
    }
}
