package org.example.core

import org.example.TestData.Mocks
import org.example.config.FolderName.FolderName
import org.example.config.TableConfig.TableRegistry
import org.example.core.Interfaces.ETL.{Extractor, Loader, Transformer, Updater}
import org.mockito.ArgumentMatchers.{any, eq => eqTo}
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ETLCycleTest extends AnyFlatSpec with Matchers with Mocks {

  val etl = new ETLCycle(mockSpark, mockDBService, mockStorageService, mockWebService)
  val rawFolder: FolderName = FolderName.RawVacancies

  "ETLCycle" should "run EXTRACT correctly" in {
    val mockExtractor = mock[Extractor]

    when(mockExtractor.extract(mockSpark, mockWebService)).thenReturn(mockDatasetString)

    etl.run(
      etlPart = "extract",
      extractor = Some(mockExtractor),
      rawFolder = rawFolder
    )

    verify(mockExtractor, times(1)).extract(mockSpark, mockWebService)
    verify(mockStorageService, times(1)).writeText(mockDatasetString, rawFolder)
  }

  "ETLCycle" should "run TRANSFORM correctly" in {
    val mockTransformer = mock[Transformer]

    when(mockStorageService.readText(mockSpark, rawFolder)).thenReturn(mockDatasetString)
    when(mockTransformer.toRows(mockSpark, mockDatasetString)).thenReturn(mockDataFrame)
    when(mockTransformer.transform(mockSpark, mockDataFrame))
      .thenReturn(Map(
        rawFolder -> mockDataFrame,
        FolderName.Fields -> mockDataFrame
      ))

    etl.run(
      etlPart = "transform",
      transformer = Some(mockTransformer),
      rawFolder = rawFolder
    )

    verify(mockStorageService, times(1)).readText(mockSpark, rawFolder)
    verify(mockTransformer, times(1)).toRows(mockSpark, mockDatasetString)
    verify(mockTransformer, times(1)).transform(mockSpark, mockDataFrame)
    verify(mockStorageService, times(1)).write(mockDataFrame, rawFolder)
    verify(mockStorageService, times(1)).write(mockDataFrame, FolderName.Fields)
  }

  "ETLCycle" should "run LOAD correctly" in {
    val mockLoader = mock[Loader]
    val seqLoadDef = Seq(
      TableRegistry.Vacancies,
      TableRegistry.Fields
    )

    when(mockStorageService.read(eqTo(mockSpark), any())).thenReturn(mockDataFrame)
    when(mockLoader.tablesToLoad()).thenReturn(seqLoadDef)
    when(mockDataFrame.columns).thenReturn(Array.empty)

    etl.run(
      etlPart = "load",
      loader = Some(mockLoader)
    )

    seqLoadDef.foreach(ld => {
      verify(mockStorageService, times(1)).read(mockSpark, ld.folder)
      verify(mockDBService, times(1)).save(eqTo(mockDataFrame), eqTo(ld.folder), any(), any())
    })
  }

  "ETLCycle" should "run UPDATE correctly" in {
    val mockExtractor = mock[Extractor]
    val mockUpdater = mock[Updater]

    when(mockUpdater.updateLimit()).thenReturn(100)
    when(mockDBService.getActiveVacancies(any(), any())).thenReturn(mockDataFrame)
    when(mockExtractor.filterUnActiveVacancies(any(), any(), any())).thenReturn(mockDataFrame)

    etl.run(
      "update",
      extractor = Some(mockExtractor),
      updater = Some(mockUpdater)
    )

    verify(mockDBService, times(1)).getActiveVacancies(mockSpark, 100)
    verify(mockExtractor, times(1)).filterUnActiveVacancies(mockSpark, mockDataFrame, mockWebService)
    verify(mockDBService, times(1)).updateActiveVacancies(mockDataFrame)
  }

  "ETLCycle" should "throw exception for unknown ETL part" in {
    assertThrows[UnsupportedOperationException] {
      etl.run(etlPart = "unknown_part")
    }
  }

  "ETLCycle" should "throw exception if required module is missing" in {
    assertThrows[UnsupportedOperationException] {
      etl.run(etlPart = "extract")
    }
  }
}
