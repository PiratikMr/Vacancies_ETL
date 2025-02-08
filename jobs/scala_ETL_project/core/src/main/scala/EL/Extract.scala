package EL

import com.Config.FSConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

object Extract extends Serializable {

  def take(ss: SparkSession, conf: FSConf, fileName: String,
           isRoot: Boolean, format: String = "parquet"): Try[DataFrame] = {
    Try(
      ss.read
      .format(format)
      .load(conf.getPath(isRoot, fileName))
    )
  }
}