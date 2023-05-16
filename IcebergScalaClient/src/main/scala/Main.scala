package com.aagmon.icebergtalk

import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.DataFilesTable.DataFilesTableScan
import org.apache.iceberg.aws.glue.GlueCatalog
import org.apache.iceberg.aws.s3.S3FileIO
import org.apache.iceberg.{CatalogProperties, DataFile, IncrementalChangelogScan, Snapshot, Table}
import org.apache.iceberg.catalog.{Catalog, TableIdentifier}
import org.apache.iceberg.expressions.{Expression, Expressions}
import org.apache.iceberg.hadoop.HadoopCatalog

import java.time.temporal.{ChronoUnit, TemporalAmount}
import java.time.{Instant, LocalDate, LocalDateTime}
import scala.jdk.CollectionConverters._
import scala.util.Try

object Main {

  private def getHadoopCatalog(warehouseLocation:String): HadoopCatalog = {
    val conf = new Configuration
    new HadoopCatalog(conf, warehouseLocation)
  }

  private def getGlueCatalog(catalogName: String, warehouseLocation: String): GlueCatalog =  {
    val catalog = new GlueCatalog
    val props = Map(
      CatalogProperties.CATALOG_IMPL -> classOf[GlueCatalog].getName,
      CatalogProperties.WAREHOUSE_LOCATION -> warehouseLocation,
      CatalogProperties.FILE_IO_IMPL -> classOf[S3FileIO].getName
    ).asJava
    catalog.initialize(catalogName, props)
    catalog
  }


  private def printFileData(file:DataFile) (implicit table:Table ): Unit = {
    println(s"\t\t " +
      s"Partition: ${file.partition().toString} " +
      s"Path:${file.path} " +
      s"Bytes:${file.fileSizeInBytes} " +
      s"Partition:${file.partition().toString} " +
      s"Records:${file.recordCount}")
  }

  private def printSnapshotData(snap:Snapshot) (implicit table:Table): Unit = {
    val addedFiles = snap.addedDataFiles(table.io())
    val removedDataFiles = snap.removedDataFiles(table.io())
    println(s"\t " +
      s"TS: ${Instant.ofEpochSecond(snap.timestampMillis / 1000).toString} " +
      s"ID:${snap.snapshotId} " +
      s"OP:${snap.operation}  " +
      s"Added Files Count:${addedFiles.asScala.size} " +
      s"Added Size:${addedFiles.asScala.map(_.fileSizeInBytes()).sum} " +
      s"RemovedDataFiles:${removedDataFiles.asScala.size}")

    addedFiles.forEach(f => printFileData(f))

  }

  private def epochMilliGT(vEarly:Long, vLater:Long) = {
    Instant.ofEpochMilli(vLater)
      .isAfter(Instant.ofEpochMilli(vEarly))
  }

  private def getDataFilesMetricsx(files:Iterable[DataFile]): (Long, Long) = {
    val bytesAdded = files.map(_.fileSizeInBytes()).sum
    val recordsAdded = files.map(_.recordCount()).sum

    (bytesAdded, recordsAdded)
  }

  private case class FileOperationMetric(records:Long, bytes:Long)

  private def getDataFilesMetrics(files: Iterable[DataFile]): FileOperationMetric = {
    FileOperationMetric(files.map(_.recordCount()).sum,
      files.map(_.fileSizeInBytes()).sum)
  }
  private def runGlueCatalogQueries():Unit = {
    val catalog = getGlueCatalog("default", "s3://doesnt_matter")
    val table = catalog.loadTable(TableIdentifier.of("sega", "sega_custom_media_sources"))
    val lastRun = Instant.now().minus(6, ChronoUnit.HOURS)
    table.snapshots().asScala
      .filter(s=>epochMilliGT(lastRun.toEpochMilli, s.timestampMillis()))
      .foreach(snapshot => {
        val timeStr = Instant.ofEpochMilli(snapshot.timestampMillis).toString
        val operation = snapshot.operation
        val addOpera = getDataFilesMetrics(snapshot.addedDataFiles(table.io()).asScala)
        val remOpera = getDataFilesMetrics(snapshot.removedDataFiles(table.io()).asScala)
        println(s"$timeStr, $operation, ${addOpera.bytes},${addOpera.records}, ${remOpera.bytes},${remOpera.records}, Records Added:${addOpera.records - remOpera.records}")
      })
  }

  def main(args: Array[String]): Unit = {

    //runGlueCatalogQueries()

    runDemo(args)

  }

  def runDemo(args: Array[String]): Unit = {
    val warehouseLocation = "/Users/alonagmon/MyData/work/hayadata/warehouse"
    println(s"starting catalog on warehouse location: $warehouseLocation")
    val catalog = getHadoopCatalog(warehouseLocation)
    implicit val table: Table = catalog.loadTable(TableIdentifier.of("mydb", "customers"))
    println(s"${table.name()} table loaded")
    println("Current Snapshot:\n")
    printSnapshotData(table.currentSnapshot)
    println("Snapshots:\n")
    table.snapshots().forEach( snap => {
        printSnapshotData(snap)
    })

    println("\n\nCurrent files in table:")

    table
      .newScan()//.filter(Expressions.equal("tier", "A"))
      .planFiles().asScala
      .map(_.file()).toList
      .groupBy(_.partition().toString)
      .foreach{ case (partition, files) =>
        val totalBytes = files.map(_.fileSizeInBytes).sum
        val totalRecords = files.map(_.recordCount).sum
        println(s"\t " +
          s"Partition: $partition " +
          s"TotalBytes: $totalBytes " +
          s"TotalRecords: $totalRecords")
      }
      //.foreach(printFileData(_))
  }
}



