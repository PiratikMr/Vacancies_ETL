package com.extractURL

import com.Config.CommonConfig
import sttp.client3.{Response, SimpleHttpClient, UriContext, basicRequest}

import scala.util.Try

object ExtractURL {
  private lazy val client = SimpleHttpClient()

  def takeURL(
               url: String,
               conf: CommonConfig
             ): Try[String] = {
    basicRequest.headers(conf.headers)
    val response: Response[Either[String, String]] = client.send(basicRequest.get(uri"$url"))

    Try(
      response.body match {
        case Left(body) => throw new Exception(s"response to GET with code ${response.code}:\n$body")
        case Right(body) => body
      }
    )
  }
}
