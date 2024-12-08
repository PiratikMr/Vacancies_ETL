import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.spark.sql.SparkSession;

import java.io.*;
import java.net.URI;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;


public class Delete implements Serializable {
	SparkSession ss;
	FileSystem hdfs;

	Path filePath;
	String date;
	int minusMonths;


	public Delete() throws IOException {
		this.filePath = new Path(System.getenv("filePath"));
		this.date = System.getenv("date");
		this.minusMonths = Integer.parseInt(System.getenv("minusMonths"));


		ss = SparkSession.builder().appName("Extract Job").master("local[*]").getOrCreate();
		Configuration conf = ss.sparkContext().hadoopConfiguration();
		hdfs = FileSystem.get(URI.create(System.getenv("filePath")), conf);
	}

	public void delete() throws IOException {

		LocalDate inputDate = LocalDate.parse(date, DateTimeFormatter.ISO_LOCAL_DATE);
		LocalDate deadLine = inputDate.minusMonths(minusMonths);

		FileStatus[] files = hdfs.listStatus(filePath);

		for (FileStatus file: files) {
			Path path = file.getPath();
			String fileName = path.getName();

			if (file.isDirectory()) {
				try {
					LocalDate folderDate = LocalDate.parse(fileName, DateTimeFormatter.ISO_LOCAL_DATE);
					if (folderDate.isBefore(deadLine)) {
						if (!hdfs.delete(path, true)) {
							System.err.println("Error during deleting directory " + path);
						} else {
							System.out.println("Directory removed: " + path);
						}
					}
				} catch (DateTimeParseException ignored) {}
			}
		}

		ss.stop();
	}
}
