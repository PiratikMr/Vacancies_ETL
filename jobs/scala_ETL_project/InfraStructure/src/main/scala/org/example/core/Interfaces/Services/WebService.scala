package org.example.core.Interfaces.Services

trait WebService extends Serializable {

  def read(url: String): Either[String, String]

  def readOrDefault(url: String, default: String): String

  def readOrNone(url: String): Option[String]

  def readOrThrow(url: String): String

}