package com.files.Common

import com.files.FolderName

case class FSConf(
            url: String,
            currDate: String,
            rootPath: String,
            platform: String
            ) extends Serializable {

  def getPath(folderName: FolderName): String = {

    val fileName: String = if (folderName.isDictionary) "" else  s"/$currDate"
    s"$url/$rootPath$platform/${folderName.path}$fileName"

  }
}
