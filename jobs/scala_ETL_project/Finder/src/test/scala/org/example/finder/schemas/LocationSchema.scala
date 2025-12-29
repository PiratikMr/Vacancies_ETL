package org.example.finder.schemas

case class LocationSchema(
                           id: Long,
                           name: Option[String],
                           country: Option[String]
                         )
