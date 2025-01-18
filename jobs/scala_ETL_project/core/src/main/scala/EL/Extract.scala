package EL

import Spark.SparkApp
import com.Config.HDFSConfig
import org.apache.spark.sql.DataFrame

import scala.util.Try

object Extract extends SparkApp {

  def take(isRoot: Boolean, fileName: String, format: String = "parquet"): Try[DataFrame] = {
    Try(
      ss.read
      .format(format)
      .load(HDFSConfig.path(isRoot, fileName))
    )
  }
}