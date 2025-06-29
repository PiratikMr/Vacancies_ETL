package com.files

import sttp.client3.SttpClientException.TimeoutException
import sttp.client3.{SimpleHttpClient, UriContext, basicRequest}

import scala.concurrent.duration._

object URLHandler {

  private lazy val client = SimpleHttpClient()

  def readURL(
               url: String,
               conf: CommonConfig,
               timeout: Duration = 30.seconds
             ): Option[String] = {
    try {
      val response = client.send(
        basicRequest
          .headers(conf.headers)
          .get(uri"$url")
          .readTimeout(timeout)
      )

      response.body match {
        case Right(body) => Some(body)
        case Left(error) =>
          println(s"Request failed with code ${response.code}: $error for [$url]")
          None
      }
    } catch {
      case _: TimeoutException =>
        println(s"Request timed out (after $timeout) for [$url]")
        None
      case e: Exception =>
        println(s"Request failed with exception: ${e.getMessage} for [$url]")
        None
    }
  }


  def requestError(url: String): Nothing = {
    throw new Exception(s"Request failed for $url")
  }
}