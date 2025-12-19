package org.example.config.Cases.Structures

import org.example.config.FolderName.FolderName

case class FSConf(
            url: String,
            saveFolder: String,
            rootPath: String,
            platform: String
            ) {

  def getPath(folderName: FolderName): String =
    s"$url/$rootPath/$platform/${folderName.fullPath}/$saveFolder"
}