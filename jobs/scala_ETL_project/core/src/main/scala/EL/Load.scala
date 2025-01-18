package EL

import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.Try
import Spark.SparkApp
import com.Config.HDFSConfig

object Load extends SparkApp {
  def give(isRoot: Boolean, data: DataFrame, fileName: String, format: String = "parquet"): Try[Unit] = {
    Try(
      data.write
      .mode(SaveMode.Overwrite)
      .format(format)
      .save(HDFSConfig.path(isRoot = isRoot, fileName))
    )
  }
}