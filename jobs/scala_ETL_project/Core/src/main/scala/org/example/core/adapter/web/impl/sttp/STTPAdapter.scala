package org.example.core.adapter.web.impl.sttp

import org.example.core.adapter.web.WebAdapter
import org.example.core.adapter.web.impl.sttp.model._
import org.example.core.config.model.structures.NetworkConf
import sttp.client4.{SyncBackend, basicRequest}
import sttp.model.Uri

import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.util.{Failure, Success, Try}


class STTPAdapter(conf: NetworkConf,
                  backendProvider: () => SyncBackend) extends WebAdapter {

  @transient private lazy val backend: SyncBackend = backendProvider()


  override def execute(url: String): Either[WebError, WebResponse] = {

    val uriEither = Uri.parse(url).left.map(e => ParsingError(s"Invalid URL: $e"))

    uriEither.flatMap { uri =>
      val request = basicRequest
        .headers(conf.headers.toMap)
        .get(uri)
        .readTimeout(Duration(conf.timeout, MILLISECONDS))

      Try(request.send(backend)) match {
        case Success(response) =>
          val bodyString = response.body match {
            case Right(b) => b
            case Left(b) => b
          }

          val webResponse = WebResponse(
            body = bodyString,
            statusCode = response.code.code,
            headers = response.headers.map(h => h.name -> h.value).toMap
          )

          if (response.code.isSuccess) Right(webResponse)
          else Left(HttpError(response.code.code, bodyString))

        case Failure(exception) =>
          println(exception)
          Left(ConnectionError(exception))
      }
    }

  }

  override def readBody(url: String): Either[WebError, String] =
    execute(url).map(_.body)

  override def readBodyOrThrow(url: String): String =
    readBody(url) match {
      case Right(body) => body
      case Left(error) => throw new RuntimeException(error.getMessage, error match {
        case ConnectionError(c) => c
        case _ => null
      })
    }

  override def readBodyOrNone(url: String): Option[String] =
    readBody(url).toOption


  override def close(): Unit = backend.close()
}

object STTPAdapter {

  def apply(conf: NetworkConf, backendType: BackendType): STTPAdapter = {
    new STTPAdapter(conf, () => STTPBackendFactory.create(backendType))
  }

  def apply(conf: NetworkConf): STTPAdapter = {
    apply(conf, BackendType.Default)
  }
}