package org.example.core.config.schema

import org.apache.spark.sql.types._
import org.example.core.config.schema.SchemaRegistry.DataBase.Entities._


sealed trait BaseVacancyCols extends Schema {
  val externalId = StructField("external_id", StringType, nullable = false)
  val latitude = StructField("latitude", DoubleType)
  val longitude = StructField("longitude", DoubleType)

  val salaryFrom = StructField("salary_from", DoubleType)
  val salaryTo = StructField("salary_to", DoubleType)

  val publishedAt = StructField("published_at", TimestampType, nullable = false)
  val title = StructField("title", StringType, nullable = false)
  val url = StructField("url", StringType, nullable = false)

  protected val baseCols: Seq[StructField] = Seq(
    externalId, latitude, longitude, salaryFrom,
    salaryTo, publishedAt, title, url
  )
}

sealed trait OneToManyVacancyCols extends Schema {
  val platformId = StructField("platform_id", LongType, nullable = false)
  val employerId = StructField("employer_id", LongType)
  val currencyId = StructField("currency_id", LongType)
  val experienceId = StructField("experience_id", LongType)

  protected val refCols: Seq[StructField] = Seq(
    platformId, employerId, currencyId, experienceId
  )
}


object SchemaRegistry {

  object DataBase {
    object Entities {
      object Platform extends DataBaseOneToManyEntity("platform")
      object Employer extends DataBaseOneToManyEntity("employer")
      object Currency extends DataBaseOneToManyEntity("currency")
      object Experience extends DataBaseOneToManyEntity("experience")
      object Employments extends DataBaseManyToManyEntity("employment")
      object Schedules extends DataBaseManyToManyEntity("schedule")
      object Country extends DataBaseOneToManyEntity("country")
      object Fields extends DataBaseManyToManyEntity("field")
      object Grades extends DataBaseManyToManyEntity("grade")
      object Skills extends DataBaseManyToManyEntity("skill")

      object Languages extends DataBaseManyToManyEntity("language") {
        val levelId = StructField(s"language_level_id", LongType)
      }
      object LanguageLevels extends DataBaseOneToManyEntity("language_level")


      object Locations extends DataBaseManyToManyEntity("location") {
        override val dimTable: DataBaseDimTable = new DataBaseDimTable("location", "country_id")
      }
    }

    object FactVacancy extends DataBaseTable("fact_vacancy") with BaseVacancyCols with OneToManyVacancyCols {
      val vacancyId = StructField("vacancy_id", LongType)

      override protected val schemaFields: Seq[StructField] = baseCols ++ refCols :+ vacancyId
    }
  }


  object Internal {
    object RawVacancy extends BaseVacancyCols {
      val platform = StructField("platform", StringType, nullable = false)
      val employer = StructField("employer", StringType)
      val currency = StructField("currency", StringType)
      val experience = StructField("experience", StringType)

      val description = StructField("description", StringType)

      val employments = StructField("employments", ArrayType(StringType))
      val schedules = StructField("schedules", ArrayType(StringType))

      val locationCountry = StructField("country", StringType)
      val locationRegion = StructField("region", StringType)
      val locations = StructField("locations", ArrayType(StructType(Seq(
        locationCountry,
        locationRegion
      ))))

      val fields = StructField("fields", ArrayType(StringType))
      val grades = StructField("grades", ArrayType(StringType))
      val skills = StructField("skills", ArrayType(StringType))

      val languageLevel = StructField("level", StringType)
      val languageLanguage = StructField("language", StringType)
      val languages = StructField("languages", ArrayType(StructType(Seq(
        languageLanguage,
        languageLevel
      ))))




      override protected val schemaFields: Seq[StructField] = baseCols ++ Seq(
        platform, employer, currency, experience,
        description, employments, schedules, locations,
        fields, grades, skills, languages
      )
    }

    object NormalizedVacancy extends BaseVacancyCols with OneToManyVacancyCols {
      val employmentIds = StructField(Employments.dimTable.entityId.name, ArrayType(LongType, containsNull = false))
      val scheduleIds = StructField(Schedules.dimTable.entityId.name, ArrayType(LongType, containsNull = false))
      val locationIds = StructField(Locations.dimTable.entityId.name, ArrayType(LongType, containsNull = false))
      val fieldIds = StructField(Fields.dimTable.entityId.name, ArrayType(LongType, containsNull = false))
      val gradeIds = StructField(Grades.dimTable.entityId.name, ArrayType(LongType, containsNull = false))
      val skillIds = StructField(Skills.dimTable.entityId.name, ArrayType(LongType, containsNull = false))
//      val languageIds = StructField(Languages.dimTable.entityId.name, ArrayType(LongType, containsNull = false))

      val languageLanguage = StructField(Languages.bridge.entityId.name, LongType)
      val languageLevel = StructField(Languages.levelId.name, LongType)
      val languages = StructField(Languages.dimTable.entityId.name, ArrayType(StructType(Seq(
        languageLevel,
        languageLanguage
      ))))

      override protected val schemaFields: Seq[StructField] = baseCols ++ refCols ++ Seq(
        employmentIds, scheduleIds, locationIds, fieldIds,
        gradeIds, skillIds, languages
      )
    }
  }

}