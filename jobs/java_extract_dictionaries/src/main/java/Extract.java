import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class Extract implements Serializable {
    String filePath;

	SparkSession ss;

	public Extract() {
		this.filePath = System.getenv("filePath");
		ss = SparkSession.builder().appName("Extract").master("local[*]").getOrCreate();
	}

	public void extract() {

		//##// areas
		Dataset<Row> areas = readAreas(getDF("areas"))
				.withColumn("id", col("id").cast(DataTypes.LongType))
				.withColumn("parent_id", col("parent_id").cast(DataTypes.LongType));

		saveJson(areas, "areas");



		//##// professional_roles
		Dataset<Row> categories = getDF("professional_roles")
				.withColumn("categories", explode(col("categories")))
				.select("categories.*")
				.withColumn("id", col("id").cast(DataTypes.LongType));

		//#// roles
		Dataset<Row> roles = categories
				.withColumn("roles", explode(col("roles")))
				.select(col("id").as("parent_id"),
						col("roles").getField("id").cast(DataTypes.LongType).as("id"),
						col("roles").getField("name").as("name"));
		saveJson(roles, "roles");

		//#// categories
		saveJson(categories.drop("roles"), "categories");



		//##// dictionaries
		Dataset<Row> dicts = getDF("dictionaries");
		saveJson(dicts, "dictionaries");

		ss.stop();
	}

	// creating DataFrame
	Dataset<Row> getDF(String envVar) {
		List<String> list = Collections.singletonList(readUrl(System.getenv(envVar)));
		Dataset<String> ds = ss.createDataset(list, Encoders.STRING());
		return ss.read().json(ds);
	}


	// Reading specific fields
	Dataset<Row> readAreas(Dataset<Row> areas) {
		long nonEmptyCount = areas.agg(
				count(when(size(col("areas")).gt(0), 1)).as("nonEmptyCount")
		).first().getLong(0);

		if (nonEmptyCount == 0)
			return areas.drop("areas");

		Dataset<Row> next = areas
				.withColumn("areas", explode(col("areas")))
				.select("areas.*");

		Dataset<Row> edit = readAreas(next);

		return areas.drop("areas").union(edit);
	}

	// Reading url
	String readUrl(String url) {
		StringBuilder result = new StringBuilder();

		try {
			HttpURLConnection con = getCon(url);

			int code = con.getResponseCode();
			if (code == HttpURLConnection.HTTP_OK) {
				result = new StringBuilder();
				try (BufferedReader reader = new BufferedReader(new InputStreamReader(con.getInputStream()))) {
					String line;
					while ((line = reader.readLine()) != null) {
						result.append(line);
					}
				} catch (IOException e) {
					System.err.println(e);
				}
			} else {
				System.err.println("Error " + code + ". URL: " + url);
			}

			return result.toString();

		} catch (IOException e) {
			System.err.println(e);
		}

		return null;
	}

	HttpURLConnection getCon(String url) throws IOException {
		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();
		con.setRequestMethod("GET");
		con.setRequestProperty("User-Agent", "");
		return con;
	}


	// Saving
	void saveJson(Dataset<Row> df, String fileName) {
		df.write()
				.mode("overwrite")
				.json(filePath + fileName);
	}
}