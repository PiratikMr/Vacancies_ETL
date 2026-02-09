package org.example.finder

import org.example.TestUtils
import org.example.core.Interfaces.Services.WebService

object FinderTestObjects {

  val webService: WebService = new WebService {
    override def read(url: String): Either[String, String] = {
      val path: String = url match {
        case s"api/vacancies/?categories=1&location=all&publication_time=last_day&limit=$_&offset=$offset" =>
          s"rawClusterLimit2Offset$offset.json"
        case s"api/vacancies/$id" => s"rawVacancy$id.json"
      }

      Right(TestUtils.readFile(path))
    }

    override def readOrDefault(url: String, default: String): String =
      read(url).getOrElse(default)

    override def readOrNone(url: String): Option[String] =
      read(url).toOption

    override def readOrThrow(url: String): String = ???
  }

}
