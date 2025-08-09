package com.files.Common

import com.files.FolderName

case class DBConf(
                 name: String,
                 pass: String,
                 host: String,
                 port: String,
                 base: String,
                 platform: String
                 ) extends Serializable {
  val url: String = s"jdbc:postgresql://$host:$port/$base"

  final def getDBTableName(folderName: FolderName): String = {
    val name: String = folderName.getDBName

    folderName match {
      case FolderName.Currency => name
      case _ => s"${platform}_$name"
    }
  }
}
