package com.files

import com.files.Common.URLConf
import sttp.client4.{DefaultSyncBackend, Response, SyncBackend, UriContext, WebSocketSyncBackend, basicRequest}
import sttp.model.StatusCode

import scala.collection.GenTraversableOnce
import scala.concurrent.duration._
import scala.language.higherKinds



object URLHandler extends Serializable {

  // error response in case of an exception
  private def errorResponse(e: Exception): Response[Either[String, String]] = Response(
    Left(e.getMessage),
    StatusCode.InternalServerError,
    null
  )

  /**reading url sync*/
  def read(
            conf: URLConf,
            url: String,
            backend: Option[SyncBackend] = None
             ): Response[Either[String, String]] = {

    val back = backend.getOrElse(DefaultSyncBackend())

    try {
      basicRequest
        .headers(conf.headers.toMap)
        .get(uri"$url")
        .readTimeout(Duration(conf.timeout, MILLISECONDS))
        .send(back)
    } catch {
      case e: Exception => errorResponse(e)
    } finally {
      if (backend.isEmpty) back.close()
    }

  }


  def useClient[T, K](iterator: Iterator[T], func: (WebSocketSyncBackend, T) => GenTraversableOnce[K]): Iterator[K] = {
    val backend = DefaultSyncBackend()
    try {
      iterator.flatMap(v => func(backend, v))
    } finally { backend.close() }
  }


  /**reading url with response's processing*/
  // in case of an error print it and return None

  def readOrNone(conf: URLConf, url: String, backend: Option[SyncBackend] = None): Option[String] = {
    read(conf, url, backend).body match {
      case Right(body) => Some(body)
      case Left(e) =>
        println(s"Failed while reading [$url] := $e")
        None
    }
  }


  // in case of an error print it and return Default value

  def readOrDefault(conf: URLConf, url: String, value: String = "", backend: Option[SyncBackend] = None): String = {
    read(conf, url, backend).body match {
      case Right(body) => body
      case Left(e) =>
        println(s"Failed while reading [$url] := $e")
        value
    }
  }


  // in case of an error throw exception

  def readOrThrow(conf: URLConf, url: String, backend: Option[SyncBackend] = None): String = {
    read(conf, url, backend).body match {
      case Right(body) => body
      case Left(e) => throw new Exception(e)
    }
  }

}