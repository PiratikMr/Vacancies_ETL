package org.example.TestData

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.example.core.Interfaces.Services.{DataBaseService, StorageService, WebService}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar

trait Mocks extends MockitoSugar {

  val mockSpark: SparkSession = mock[SparkSession]

//  implicit lazy val localSpark: SparkSession = SparkSession.builder()
//    .master("local[*]")
//    .appName("TestName")
//    .config("spark.driver.bindAddress", "127.0.0.1")
//    .config("spark.driver.host", "127.0.0.1")
//    .config("spark.ui.enabled", "false")
//    .getOrCreate()

  val mockDataFrame: DataFrame = mock[DataFrame]
  val mockDatasetString: Dataset[String] = mock[Dataset[String]]

  val mockDBService: DataBaseService = mock[DataBaseService]
  val mockStorageService: StorageService = mock[StorageService]
  val mockWebService: WebService = mock[WebService]
}
