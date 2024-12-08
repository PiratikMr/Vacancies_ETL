import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.*;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;

public class Load implements Serializable {
	String filePath;
	SparkSession ss;

	Dataset<Row> dictionaries;

	public Load() {
		this.filePath = System.getenv("filePath");
		ss = SparkSession.builder().appName("Transform Job").master("local[*]").getOrCreate();
	}

	public void load() {

		//##// areas
		writeDF("areas");

		//##// categories
		writeDF("categories");

		//##// roles
		writeDF("roles");

		//##// dictionaries
		dictionaries = ss.read().json(filePath + "dictionaries");

		writeDictionary("employment");
		writeDictionary("experience");
		Dataset<Row> schedule = dictionaries
				.withColumn("schedule", explode(col("schedule")))
				.select("schedule.*")
				.drop("uid");
		write(schedule, "schedule");


        ss.stop();
	}

	void writeDF(String name) {
		Dataset<Row> data = ss.read().json(filePath + name);
		write(data, name);
	}

	void writeDictionary (String dictionary) {
		Dataset<Row> df = dictionaries
				.withColumn(dictionary, explode(col(dictionary)))
				.select(dictionary + ".*");
		write(df, dictionary);
	}

	void write (Dataset<Row> data, String tableName) {
		data.write()
				.format("jdbc")
				.option("truncate", true)
				.option("driver", "org.postgresql.Driver")
				.option("url", System.getenv("url"))
				.option("dbtable", tableName)
				.option("user", System.getenv("user"))
				.option("password", System.getenv("password"))
				.mode(System.getenv("mode"))
				.save();
	}
}
