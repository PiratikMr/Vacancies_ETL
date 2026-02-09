package org.example.finder

import org.example.finder.schemas.{FieldSchema, FullDataSchema, LocationSchema, VacancySchema}

import java.sql.Timestamp
import java.time.Instant

object FinderTestUtils {

  def createDataRow (
                     id: Int
                   ): FullDataSchema =
    {
      val longId = id.toLong
      FullDataSchema(createVacancy(longId), createLocations(longId), createFields(longId))
    }

  def createVacancy(id: Long): VacancySchema = VacancySchema(
    id = id,
    title = Some(s"title_$id"),
    employment_type = Some(s"employment_type_$id"),
    salary_from = Some(id * 10L),
    salary_to = Some(id * 100L),
    salary_currency_id = Some(s"salary_currency_id_$id"),
    published_at = Timestamp.from(Instant.parse("2025-01-01T00:00:00.000000Z")),
    url = s"https://finder.work/vacancies/$id",
    experience = Some(s"experience_$id"),
    distant_work = Some(true),
    employer = Some(s"employer_$id"),
    address_lat = Some(0.0005),
    address_lng = Some(0.0005),
    closed_at = None
  )

  def createLocations(id: Long): Seq[LocationSchema] = {
    (1L to id).map(vl => LocationSchema(
      id = id,
      name = Some(s"name_$vl"),
      country = Some(s"country_$vl")
    ))
  }

  def createFields(id: Long): Seq[FieldSchema] = {
    (1L to id).map(vl => FieldSchema(
      id = id,
      name = Some(s"field_$vl")
    ))
  }
}
