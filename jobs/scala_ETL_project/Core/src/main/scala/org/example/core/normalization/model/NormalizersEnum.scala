package org.example.core.normalization.model

import org.example.core.config.database._
import org.example.core.etl.model.VacancyColumns

object NormalizersEnum {

  sealed trait NormalizerType {
    def mappedIdCol: String
    def sourceCol: String // Колонка в сырой Vacancy, откуда берем данные
    def isArray: Boolean  // Флаг, нужно ли группировать результат в массив
  }

  sealed trait GroupNonHierarchical extends NormalizerType
  sealed trait GroupHierarchical extends NormalizerType

  case object CURRENCY    extends GroupNonHierarchical { override def mappedIdCol: String = FactVacancyDef.currencyId; override def sourceCol: String = VacancyColumns.CURRENCY; override def isArray: Boolean = false }
  case object EMPLOYER    extends GroupNonHierarchical { override def mappedIdCol: String = FactVacancyDef.employerId; override def sourceCol: String = VacancyColumns.EMPLOYER; override def isArray: Boolean = false }
  case object EXPERIENCE  extends GroupNonHierarchical { override def mappedIdCol: String = FactVacancyDef.experienceId; override def sourceCol: String = VacancyColumns.EXPERIENCE; override def isArray: Boolean = false }
  case object PLATFORM    extends GroupNonHierarchical { override def mappedIdCol: String = FactVacancyDef.platformId; override def sourceCol: String = VacancyColumns.PLATFORM; override def isArray: Boolean = false }

  case object EMPLOYMENTS extends GroupNonHierarchical { override def mappedIdCol: String = BridgeVacancyEmploymentDef.entityId; override def sourceCol: String = VacancyColumns.EMPLOYMENTS; override def isArray: Boolean = true }
  case object FIELDS      extends GroupNonHierarchical { override def mappedIdCol: String = BridgeVacancyFieldDef.entityId; override def sourceCol: String = VacancyColumns.FIELDS; override def isArray: Boolean = true }
  case object GRADES      extends GroupNonHierarchical { override def mappedIdCol: String = BridgeVacancyGradeDef.entityId; override def sourceCol: String = VacancyColumns.GRADES; override def isArray: Boolean = true }
  case object SCHEDULES   extends GroupNonHierarchical { override def mappedIdCol: String = BridgeVacancyScheduleDef.entityId; override def sourceCol: String = VacancyColumns.SCHEDULES; override def isArray: Boolean = true }
  case object SKILLS      extends GroupNonHierarchical { override def mappedIdCol: String = BridgeVacancySkillDef.entityId; override def sourceCol: String = VacancyColumns.SKILLS; override def isArray: Boolean = true }

  case object COUNTRY     extends GroupNonHierarchical { override def mappedIdCol: String = DimCountryDef.entityId; override def sourceCol: String = ""; override def isArray: Boolean = false }
  case object LOCATIONS   extends GroupHierarchical { override def mappedIdCol: String = BridgeVacancyLocationDef.entityId; override def sourceCol: String = VacancyColumns.LOCATIONS; override def isArray: Boolean = true }

  case object LANGUAGES   extends GroupNonHierarchical { override def mappedIdCol: String = BridgeVacancyLanguageDef.entityId; override def sourceCol: String = VacancyColumns.LANGUAGES; override def isArray: Boolean = true }
  case object LANGUAGES_LEVEL extends GroupNonHierarchical { override def mappedIdCol: String = BridgeVacancyLanguageDef.languageLevelId; override def sourceCol: String = ""; override def isArray: Boolean = false }

  val values: Set[NormalizerType] = Set(
    CURRENCY, EMPLOYER, EXPERIENCE, PLATFORM, EMPLOYMENTS,
    FIELDS, GRADES, SCHEDULES, SKILLS, COUNTRY, LOCATIONS, LANGUAGES,
    LANGUAGES_LEVEL
  )

  lazy val nonHierarchical: Set[GroupNonHierarchical] = values.collect { case x: GroupNonHierarchical => x }
  lazy val hierarchical: Set[GroupHierarchical] = values.collect { case x: GroupHierarchical => x }
}