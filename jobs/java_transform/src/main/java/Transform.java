import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple2;

import java.io.*;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

public class Transform implements Serializable {
	String filePath;

	SparkSession ss;

	Dataset<Row> items;

	Map<String, Double> currency;
	Map<Long, Area> areas;

	public Transform() {
		this.filePath = System.getenv("filePath");
		init();
	}

	public void init() {
		ss = SparkSession.builder().appName("Transform Job").master("local[*]").getOrCreate();

		items = ss.read().json(filePath + System.getenv("date") + "vacancies*");


		// currency
		currency = ss.read().json(filePath + "dictionaries")
				.withColumn("currency", explode(col("currency")))
				.select("currency.*")
				.toJavaRDD()
				.mapToPair((PairFunction<Row, String, Double>) row -> {
					String key = row.getString(row.fieldIndex("code"));
					Double rate = row.getDouble(row.fieldIndex("rate"));
					return new Tuple2<>(key, rate);
				}).collectAsMap();

		/*JavaRDD<Row> curr = ss.read().json(filePath + "dictionaries")
				.withColumn("currency", explode(col("currency")))
				.select("currency.*")
				.toJavaRDD();
		JavaPairRDD<String, Double> curMap = curr.mapToPair((PairFunction<Row, String, Double>) row -> {
            		String key = row.getString(row.fieldIndex("code"));
            		Double rate = row.getDouble(row.fieldIndex("rate"));
            		return new Tuple2<>(key, rate);
        });
		currency = curMap.collectAsMap();*/


		// areas
		areas = ss.read().json(filePath + "areas").toJavaRDD()
				.mapToPair((PairFunction<Row, Long, Area>) row -> {
					Long id = row.getLong(row.fieldIndex("id"));
					Area area = new Area();
					area.name = row.getString(row.fieldIndex("name"));
					if (row.isNullAt(row.fieldIndex("parent_id")))
						area.parent_id = null;
					else
						area.parent_id = row.getLong(row.fieldIndex("parent_id"));
					return new Tuple2<>(id, area);
				}).collectAsMap();

		/*JavaRDD<Row> ars = ss.read().json(filePath + "areas").toJavaRDD();

		JavaPairRDD<Long, Area> arsMap = ars.mapToPair((PairFunction<Row, Long, Area>) row -> {
			Long id = row.getLong(row.fieldIndex("id"));
			Area area = new Area();
			area.name = row.getString(row.fieldIndex("name"));
			if (row.isNullAt(row.fieldIndex("parent_id")))
				area.parent_id = null;
			else
				area.parent_id = row.getLong(row.fieldIndex("parent_id"));
			return new Tuple2<>(id, area);
		});
		areas = arsMap.collectAsMap();*/
	}

	public void transform() {

		Dataset<Row> finals = items
				.withColumn("id", col("id").cast(DataTypes.LongType))

				.withColumn("region_area_id", getString.apply(col("area"),
						lit("id")).cast(DataTypes.LongType))
				.withColumn("country_area_id", country.apply(col("area")))

				.withColumn("currency", getString.apply(col("salary"),
						lit("currency")))
				.withColumn("salary_from_local", getLong.apply(col("salary"),
						lit("from")))
				.withColumn("salary_to_local", getLong.apply(col("salary"),
						lit("to")))
				.withColumn("salary_from_rub", salary_rub.apply(col("salary"),
						lit("from")))
				.withColumn("salary_to_rub",  salary_rub.apply(col("salary"),
						lit("to")))

				.withColumn("close_to_metro", close_to_metro.apply(col("address")))

				/*.withColumn("requirement", getString.apply(col("snippet"),
						lit("requirement")))
				.withColumn("responsibility", getString.apply(col("snippet"),
						lit("responsibility")))*/

				.withColumn("schedule_id", getString.apply(col("schedule"),
						lit("id")))
				.withColumn("experience_id", getString.apply(col("experience"),
						lit("id")))
				.withColumn("employment_id", getString.apply(col("employment"),
						lit("id")))

				.select("id", "region_area_id", "country_area_id", "currency",
						"salary_from_local", "salary_to_local", "salary_from_rub", "salary_to_rub",
						"close_to_metro", "schedule_id", "experience_id", "employment_id")

				.dropDuplicates("id");

		finals.write()
				.mode("overwrite")
				.parquet(filePath + System.getenv("date") + "transformed");

        ss.stop();
	}

	// default
	UserDefinedFunction getLong = udf((Row row, String field) -> {
		if (row == null) return null;
		int i = row.fieldIndex(field);

		if (row.isNullAt(i)) return null;
		return row.getLong(i);
	}, DataTypes.LongType);

	UserDefinedFunction getString = udf((Row row, String field) -> {
		if (row == null) return null;
		int i = row.fieldIndex(field);

		if (row.isNullAt(i)) return null;
		return row.getString(i);
	}, DataTypes.StringType);

	// salary in rub
	UserDefinedFunction salary_rub = udf((Row salary, String field) -> {
		if (salary == null) return null;
		int c = salary.fieldIndex("currency");
		int i = salary.fieldIndex(field);

		if (salary.isNullAt(c) || salary.isNullAt(i)) return null;

		double rate = currency.get(salary.getString(c));
        return (long) (salary.getLong(i) / rate);
	}, DataTypes.LongType);

	// metro
	UserDefinedFunction close_to_metro = udf((Row address) -> {
		if (address == null) return null;
		int i = address.fieldIndex("metro");
		return !address.isNullAt(i);
 	}, DataTypes.BooleanType);

	// country
	UserDefinedFunction country = udf((Row area) -> {
		if (area == null) return null;

		long id = Long.parseLong(area.getString(area.fieldIndex("id")));
		Area curr = areas.get(id);

		while (curr.parent_id != null) {
			id = curr.parent_id;
			curr = areas.get(curr.parent_id);
		}

		return id;
	}, DataTypes.LongType);
}
