package EL

import com.Config.ProjectConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

object Extract extends Serializable {

  def take(
            ss: SparkSession,
            conf: ProjectConfig,
            fileName: String,
            isRoot: Boolean,
            format: String = "parquet"
          ): Try[DataFrame] = {
    Try(
      ss.read
      .format(format)
      .load(conf.fs.getPath(isRoot, fileName))
    )
  }
}