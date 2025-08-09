package com.files

import com.files.Common.URLConf
import sttp.client3.{HttpClientSyncBackend, Response, SttpBackend, UriContext, basicRequest}
import sttp.model.StatusCode
import sttp.shared.Identity

import scala.concurrent.duration._
import scala.language.higherKinds


object URLHandler {

  // error response in case of an exception
  private def errorResponse(e: Exception): Response[Either[String, String]] = Response(
    Left(e.getMessage),
    StatusCode.InternalServerError
  )

  /**reading url sync*/
  def read(
               url: String,
               conf: URLConf
             ): Response[Either[String, String]] = {

    val backend: SttpBackend[Identity, Any] = HttpClientSyncBackend()

    try {
      basicRequest
        .headers(conf.headers)
        .get(uri"$url")
        .readTimeout(Duration(conf.timeout, MILLISECONDS))
        .send(backend)
    } catch {
      case e: Exception => errorResponse(e)
    } finally {
      backend.close()
    }

  }



  /**reading url with response's processing*/
  // in case of an error print it and return None

  def readOrNone(url: String, conf: URLConf): Option[String] = {
    read(url, conf).body match {
      case Right(body) => Some(body)
      case Left(e) =>
        println(s"Failed while reading [$url] := $e")
        None
    }
  }


  // in case of an error print it and return Default value

  def readOrDefault(url: String, conf: URLConf, value: String = ""): String = {
    read(url, conf).body match {
      case Right(body) => body
      case Left(e) =>
        println(s"Failed while reading [$url] := $e")
        value
    }
  }


  // in case of an error throw exception

  def readOrThrow(url: String, conf: URLConf): String = {
    read(url, conf).body match {
      case Right(body) => body
      case Left(e) => throw new Exception(e)
    }
  }

}