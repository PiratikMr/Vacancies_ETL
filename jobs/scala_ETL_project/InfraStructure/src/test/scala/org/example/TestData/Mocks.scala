package org.example.TestData

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.example.core.Interfaces.Services.{DataBaseService, StorageService, WebService}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar

trait Mocks extends MockitoSugar {

  val mockSpark: SparkSession = mock[SparkSession]

  val mockDataFrame: DataFrame = mock[DataFrame]
  val mockDatasetString: Dataset[String] = mock[Dataset[String]]

  val mockDBService: DataBaseService = mock[DataBaseService]
  val mockStorageService: StorageService = mock[StorageService]
  val mockWebService: WebService = mock[WebService]
}
