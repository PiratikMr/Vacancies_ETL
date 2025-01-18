import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, explode, udf, lit}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

case class Area(name: String, parent_id: Option[Long])

// val area = Area.upply

class Transform(filePath: String, ss: SparkSession) extends Serializable {
  private val items: Dataset[Row] = ss.read.json(s"$filePath${sys.env("date")}vacancies*")

  private var currency: Map[String, Double] = _
  private var areas: Map[Long, Area] = _

  def this() = {
    this(
      sys.env("filePath"),
      SparkSession
        .builder()
        .appName("Transform Job")
        .master("local[*]")
        .getOrCreate())

    // currency
    currency = ss.read.
      json(s"${filePath}dictionaries").
      withColumn("currency", explode(col("currency"))).
      select("currency.*").rdd.
      map(row => {
        val key = row.getAs[String]("code")
        val rate = row.getAs[Double]("rate")
        key -> rate
      }).collectAsMap().toMap

    // areas
    areas = ss.read.
      json(s"${filePath}areas").rdd.
      map(row => {
        val id = row.getAs[Long]("id")
        val name = row.getAs[String]("name")
        val parent_id = Option(row.getAs[Long]("parent_id"))
        id -> Area(name, parent_id)
      }).collectAsMap().toMap
  }

  def main(args: Array[String]): Unit = {
    val transform = new Transform()
    transform.transform()
  }

  private def transform(): Unit = {
    val finals = items
      .withColumn("id", col("id").cast("long"))
      .withColumn("region_area_id", getStringUDF(col("area"), lit("id")).cast("long"))
      .withColumn("country_area_id", countryUDF(col("area")))
      .withColumn("currency", getStringUDF(col("salary"), lit("currency")))
      .withColumn("salary_from_local", getLongUDF(col("salary"), lit("from")))
      .withColumn("salary_to_local", getLongUDF(col("salary"), lit("to")))
      .withColumn("salary_from_rub", salaryRubUDF(col("salary"), lit("from")))
      .withColumn("salary_to_rub", salaryRubUDF(col("salary"), lit("to")))
      .withColumn("close_to_metro", closeToMetroUDF(col("address")))
      .withColumn("schedule_id", getStringUDF(col("schedule"), lit("id")))
      .withColumn("experience_id", getStringUDF(col("experience"), lit("id")))
      .withColumn("employment_id", getStringUDF(col("employment"), lit("id")))
      .select(
        "id", "region_area_id", "country_area_id", "currency",
        "salary_from_local", "salary_to_local", "salary_from_rub", "salary_to_rub",
        "close_to_metro", "schedule_id", "experience_id", "employment_id"
      )
      .dropDuplicates("id")

    finals.write
      .mode("overwrite")
      .parquet(s"$filePath${sys.env("date")}transformed")

    ss.stop()
  }

  private val getLongUDF: UserDefinedFunction = udf((row: Row, field: String) => {
    Option(row).flatMap(r => {
      val idx = r.fieldIndex(field)
      if (!r.isNullAt(idx)) Option(r.getAs[Long](idx)) else None
    })
  })

  private val getStringUDF: UserDefinedFunction = udf((row: Row, field: String) => {
    Option(row).flatMap(r => {
      val idx = r.fieldIndex(field)
      if (!r.isNullAt(idx)) Option(r.getAs[String](idx)) else None
    })
  })

  private val closeToMetroUDF: UserDefinedFunction = udf((address: Row) => {
    Option(address).exists(a => {
      val metroIdx = a.fieldIndex("metro")
      !a.isNullAt(metroIdx)
    })
  })

  private val salaryRubUDF: UserDefinedFunction = udf((salary: Row, field: String) => {
    Option(salary).flatMap(s => {
      val currencyIdx = s.fieldIndex("currency")
      val fieldIdx = s.fieldIndex(field)
      if (!s.isNullAt(currencyIdx) && !s.isNullAt(fieldIdx)) {
        val rate = currency.getOrElse(s.getAs[String](currencyIdx), 1.0)
        Option((s.getAs[Long](fieldIdx) / rate).toLong)
      } else None
    })
  })

  private val countryUDF: UserDefinedFunction = udf((area: Row) => {
    Option(area).flatMap(a => {
      var id = a.getAs[String]("id").toLong
      var currentArea = areas.get(id)
      while (currentArea.exists(_.parent_id.isDefined)) {
        id = currentArea.flatMap(_.parent_id).get
        currentArea = areas.get(id)
      }
      Option(id)
    })
  })
}