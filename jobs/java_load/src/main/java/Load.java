import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.*;

public class Load implements Serializable {
	String filePath;

	SparkSession ss;

	Dataset<Row> items;

	public Load() {
		this.filePath = System.getenv("filePath");
		ss = SparkSession.builder().appName("Transform Job").master("local[*]").getOrCreate();
	}

	public void load() {

		items = ss.read().parquet(filePath + "transformed");

		items.write()
				.format("jdbc")
				.option("truncate", true)
				.option("driver", "org.postgresql.Driver")
				.option("url", System.getenv("url"))
				.option("dbtable", "vacancies")
				.option("user", System.getenv("user"))
				.option("password", System.getenv("password"))
				.mode("append")
				.save();

        ss.stop();
	}
}
