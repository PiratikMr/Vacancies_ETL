package org.example.config.Cases.Structures

import org.example.config.FolderName.FolderName


case class DBConf(
                 name: String,
                 pass: String,
                 host: String,
                 port: String,
                 base: String,
                 platform: String
                 ) {
  def url: String = s"jdbc:postgresql://$host:$port/$base"

  def getDBTableName(folderName: FolderName): String =
    s"${platform}_${folderName.folderName.toLowerCase}"
}