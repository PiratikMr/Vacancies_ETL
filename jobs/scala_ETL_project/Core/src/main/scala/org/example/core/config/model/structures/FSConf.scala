package org.example.core.config.model.structures

case class FSConf(
            url: String,
            rootPath: String,
            platform: String,
            currDate: String
            ) {

  def getPath(folderName: String, withDate: Boolean = true): String = {

    val base = s"$url/$rootPath/$platform/$folderName"

    if (withDate) s"$base/$currDate"
    else base
  }
}