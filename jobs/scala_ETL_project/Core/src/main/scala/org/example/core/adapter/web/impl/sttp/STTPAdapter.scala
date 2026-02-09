package org.example.core.adapter.web.impl.sttp

import org.example.core.adapter.web.WebAdapter
import org.example.core.config.model.structures.NetworkConf
import sttp.client4.{SyncBackend, UriContext, basicRequest}

import scala.concurrent.duration.{Duration, MILLISECONDS}

class STTPAdapter(
                   conf: NetworkConf,
                   backendProvider: () => SyncBackend
                 ) extends WebAdapter {

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
        case Left(error) =>
          val er = s"HTTP Error: $error, Status: ${response.code}"
          println(er)
          Left(er)
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
