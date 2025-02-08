package EL

import com.Config.FSConf
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.Try

object Load extends Serializable {
  def give(conf: FSConf, fileName: String, isRoot: Boolean, data: DataFrame, format: String = "parquet"): Try[Unit] = {
    Try(
      data.write
      .mode(SaveMode.Overwrite)
      .format(format)
      .save(conf.getPath(isRoot, fileName))
    )
  }
}