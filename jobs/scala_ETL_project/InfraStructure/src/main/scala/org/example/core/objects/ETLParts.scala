package org.example.core.objects

object ETLParts extends Enumeration {
  type ETLPart = Value

  val EXTRACT, TRANSFORM, LOAD, UPDATE, UNRECOGNIZED = Value

  def parseString(str: String): ETLPart =
    str.trim.toLowerCase match {
      case "extract" => EXTRACT
      case "transform" => TRANSFORM
      case "load" => LOAD
      case "update" => UPDATE
      case _ => UNRECOGNIZED
    }
}
