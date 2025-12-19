package org.example.core.implement.Network

import org.example.core.implement.Network.STTPBackends.{DEFAULT, STTPBackend, UNSAFE}
import sttp.client4.httpclient.HttpClientSyncBackend
import sttp.client4.{DefaultSyncBackend, SyncBackend}

import java.net.http.HttpClient
import java.security.SecureRandom
import java.security.cert.X509Certificate
import javax.net.ssl.{SSLContext, TrustManager, X509TrustManager}

object STTPBackendContext {


  def getBackend(sttpBackend: STTPBackend): SyncBackend = {
    sttpBackend match {
      case DEFAULT => default
      case UNSAFE => unsafe
    }
  }


  private lazy val default: SyncBackend = withAutoClose(DefaultSyncBackend())

  private lazy val unsafe: SyncBackend = withAutoClose {
    val client = HttpClient.newBuilder().sslContext(unsafeSslContext).build()
    HttpClientSyncBackend.usingClient(client)
  }




  private def withAutoClose[T <: SyncBackend](backend: T): T = {
    sys.addShutdownHook {
      try backend.close() catch { case _: Exception => }
    }
    backend
  }

  private lazy val unsafeSslContext: SSLContext = {
    val trustAllCerts = Array[TrustManager](new X509TrustManager {
      override def getAcceptedIssuers: Array[X509Certificate] = new Array(0)
      override def checkClientTrusted(certs: Array[X509Certificate], authType: String): Unit = {}
      override def checkServerTrusted(certs: Array[X509Certificate], authType: String): Unit = {}
    })
    val sslContext = SSLContext.getInstance("TLS")
    sslContext.init(null, trustAllCerts, new SecureRandom())
    sslContext
  }
}
