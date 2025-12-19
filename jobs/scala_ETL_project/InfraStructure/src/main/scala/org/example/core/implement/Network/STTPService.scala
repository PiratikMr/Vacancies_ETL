package org.example.core.implement.Network

import org.example.config.Cases.Structures.NetworkConf
import org.example.core.Interfaces.Services.WebService
import sttp.client4.{SyncBackend, UriContext, basicRequest}

import scala.concurrent.duration.{Duration, MILLISECONDS}

class STTPService(
                   conf: NetworkConf,
                   backendProvider: () => SyncBackend
                 ) extends WebService {

  @transient lazy val backend: SyncBackend = backendProvider()

  override def read(url: String): Either[String, String] = {
    try {
      val request = basicRequest
        .headers(conf.headers.toMap)
        .get(uri"$url")
        .readTimeout(Duration(conf.timeout, MILLISECONDS))

      val response = request.send(backend)

      response.body match {
        case Right(body) => Right(body)
        case Left(error) => Left(s"HTTP Error: $error, Status: ${response.code}")
      }

    } catch {
      case e: Exception =>
        println(s"Exception calling $url: ${e.getMessage}")
        Left(e.getMessage)
    }
  }

  override def readOrDefault(url: String, default: String): String =
    read(url).getOrElse(default)

  override def readOrNone(url: String): Option[String] =
    read(url).toOption

  override def readOrThrow(url: String): String =
    read(url).getOrElse(throw new RuntimeException(s"Error during HTTP call to: $url"))
}
