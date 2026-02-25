package org.example.core.normalization.impl.model

object NormalizerColumns {
  val ENTITY_ID = "entityId"
  val MAPPED_ID = "mappedId"
}

case class VacancyTags(
                        entityId: String,
                        mappedId: Long
                      )

// Добавь этот класс к остальным контрактам
case class NormLanguageMatch(
                              entityId: String,
                              languageId: Long,
                              levelId: Option[Long]
                            )