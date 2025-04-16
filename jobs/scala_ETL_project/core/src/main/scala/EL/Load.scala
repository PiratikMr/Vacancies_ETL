package EL

import com.Config.FolderName.FolderName
import com.Config.ProjectConfig
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.Try

object Load extends Serializable {
  def give(
            conf: ProjectConfig,
            fileName: String = null,
            folderName: FolderName,
            data: DataFrame,
            format: String = "parquet"
          ): Try[Unit] = {
    Try(
      data.write
      .mode(SaveMode.Overwrite)
      .format(format)
      .save(conf.fs.getPath(folderName, fileName))
    )
  }
}