import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.io.*;
import java.lang.reflect.Array;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;

public class Extract implements Serializable {
    SparkSession ss;
	List<Long> ids;

	long PRid;

	int pages;
	int vacancies;
	String filePath;

	int pagesCount;

	public Extract() {
		this.pages = Integer.parseInt(System.getenv("pages"));
		this.vacancies = Integer.parseInt(System.getenv("vacancies"));
		this.filePath = System.getenv("filePath");
		this.PRid = Long.parseLong(System.getenv("PRid"));

		this.pagesCount = 0;

		init();
	}

	void init() {
		ss = SparkSession.builder().appName("Extract Job").master("local[*]").getOrCreate();

		JavaRDD<Row> roles = ss.read().json(filePath + "roles")
				.where(col("parent_id").equalTo(PRid))
				.toJavaRDD();

		ids = roles.map((Function<Row, Long>) row -> row.getLong(row.fieldIndex("id"))).collect();

		System.out.println("count of id: " + ids.size());
	}

	public void extract() {

		String url1 = System.getenv("url1");
		String url2 = System.getenv("url2");
		String url3 = System.getenv("url3");
		String file = filePath + System.getenv("date") + "vacancies";

		String res;
		ObjectMapper mapper = new ObjectMapper();
		JsonNode node;

		/*for (Long id: ids) {
			List<String> list = new ArrayList<>();
			long localPages = pages;

			res = readUrl(url1 + 0 + url2 + 0 + url3 + id);
			if (res != null) {
				try {
					node = mapper.readTree(res);
					localPages = Math.min(node.get("found").asLong() / vacancies, pages);
				} catch (JsonProcessingException e) {
					System.err.println(e);
				}
			}


			for (int i = 0; i < localPages; i++) {
				res = readUrl(url1 + i + url2 + vacancies + url3 + id);

				list.add(res);
			}

			System.out.println(id + ": " + list.size());
			pagesCount += list.size();

			if (!list.isEmpty()) {
				Dataset<String> ds = ss.createDataset(list, Encoders.STRING());
				Dataset<Row> df = ss.read().json(ds)
						.withColumn("items", explode(col("items")))
						.select("items.*");

				df.write()
						.mode("overwrite")
						.json(file + id);
			}
		}*/

		List<String> list = new ArrayList<>();

		for (Long id: ids) {
			long localPages = pages;

			res = readUrl(url1 + 0 + url2 + 0 + url3 + id);
			if (res != null) {
				try {
					node = mapper.readTree(res);
					localPages = Math.min(node.get("found").asLong() / vacancies, pages);
				} catch (JsonProcessingException e) {
					System.err.println(e);
				}
			}

			for (int i = 0; i < localPages; i++) {
				res = readUrl(url1 + i + url2 + vacancies + url3 + id);
				list.add(res);
			}

			System.out.println(id + ": " + localPages);
			pagesCount += (int) localPages;
		}

		if (!list.isEmpty()) {
			Dataset<String> ds = ss.createDataset(list, Encoders.STRING());
			Dataset<Row> df = ss.read().json(ds)
					.withColumn("items", explode(col("items")))
					.select("items.*");

			df.write()
					.mode("overwrite")
					.json(file);
		}

		System.out.println("Pages: " + pagesCount);

		ss.stop();
	}

	/*String readUrl(String url) {
		StringBuilder result = new StringBuilder();
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(new URL(url).openStream()))) {
			String line;
			while ((line = reader.readLine()) != null) {
				result.append(line);
			}
		} catch (IOException e) {
            log.error("e: ", e);
			return null;
        }
        return result.toString();
	}*/

	String readUrl(String url) {
		StringBuilder result = new StringBuilder();
		try {
			HttpURLConnection con = getCon(url);

			int responseCode = con.getResponseCode();
			if (responseCode == HttpURLConnection.HTTP_FORBIDDEN) {
				try {
					System.out.println("error 403: sleep - 1 sec");
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					System.err.println("Interrupted exception: " + e);
				}

				con = getCon(url);
				responseCode = con.getResponseCode();
			}

			if (responseCode == HttpURLConnection.HTTP_OK) {
				try (BufferedReader reader = new BufferedReader(new InputStreamReader(con.getInputStream()))) {
					String line;
					while ((line = reader.readLine()) != null) {
						result.append(line);
					}
				}
			} else {
				System.err.println("HTTP Response Code: " + responseCode);
				return null;
			}
		} catch (IOException e) {
			System.err.println("Error reading URL: " + url);
			e.printStackTrace();
			return null;
		}
		return result.toString();
	}

	HttpURLConnection getCon(String url) throws IOException {
		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();
		con.setRequestMethod("GET");
		con.setRequestProperty("User-Agent", "");
		return con;
	}
}
