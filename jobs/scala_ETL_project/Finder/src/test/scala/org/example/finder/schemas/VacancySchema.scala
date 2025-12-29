package org.example.finder.schemas

import java.sql.Timestamp

case class VacancySchema(
                          id: Long,
                          title: Option[String],
                          employment_type: Option[String],
                          salary_from: Option[Long],
                          salary_to: Option[Long],
                          salary_currency_id: Option[String],
                          published_at: Timestamp,
                          url: String,
                          experience: Option[String],
                          distant_work: Option[Boolean],
                          employer: Option[String],
                          address_lat: Option[Double],
                          address_lng: Option[Double],
                          closed_at: Option[Timestamp]
                        )
