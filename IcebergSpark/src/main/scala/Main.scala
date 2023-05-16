
import org.apache.iceberg.SnapshotRef
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.util.SnapshotUtil
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.temporal.ChronoUnit
import java.time.{LocalDateTime, ZoneOffset}
import scala.util.Try

object Main extends Logging{

  private def highlight(str: String): Unit = {
    log.info(s"==================== $str ====================")
  }
  private def getIcebergConfig(icebergCat: String, icebergS3WH: String): SparkConf = {
    new SparkConf()
      .set(s"spark.sql.catalog.$icebergCat", "org.apache.iceberg.spark.SparkCatalog")
      .set(s"spark.sql.catalog.$icebergCat.warehouse",icebergS3WH)
      .set(s"spark.sql.catalog.$icebergCat.type", "hadoop")
      .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  }
  private def getMasterConfig: SparkConf = {
    new SparkConf()
      .set("spark.master", "local[*]")
      .set("spark.driver.host", "127.0.0.1")
      .set("spark.driver.bindAddress", "127.0.0.1")
  }
  def main(args: Array[String]): Unit = {
    var warehousePath = "/Users/alonagmon/MyData/work/hayadata/warehouse"
    implicit val spark: SparkSession = SparkSession.builder()
      .config(getMasterConfig)
      .config(getIcebergConfig("cat", warehousePath))
      .appName("HayaDataDemo")
      .getOrCreate()

    import spark.implicits._

    highlight("Current customers table")
    spark.sql("select * from cat.mydb.customers").show(10, false)

    highlight("Current customers history table")
    val historyDF = spark.sql("select * from cat.mydb.customers.history")
    historyDF.show(10, truncate = false)

    val snapshotsList= historyDF
      .select("snapshot_id").as[String]
      .collect().toList

    highlight("Incremental read - diff between snapshots")
    spark.read
      .format("iceberg")
      .option("start-snapshot-id", snapshotsList.head)
      .option("end-snapshot-id", snapshotsList.last)
      .load("cat.mydb.customers")
      .show(10)

    highlight("CDC between snapshots")
    //getting simple cdc dataframe
    spark.sql("select * from cat.mydb.customers.changes order by _change_ordinal").show()
    // another way to get cdc dataframe
   getCDCView("mydb.customers", "cat", remove_carryovers = true).orderBy($"_change_ordinal")show(10, false)
   getCDCView("mydb.customers", "cat", remove_carryovers = false).orderBy($"_change_ordinal")show(10, false)

    highlight("Branching and Merging")
    // create tags using sparkSQL and the current snapshot
    spark.sql(s"ALTER TABLE cat.mydb.customers CREATE TAG history_tag RETAIN 7 DAYS")
    updateTagOnCurrentTable("cat.mydb.customers", "history_tag", LocalDateTime.now().plusHours(18))
    val histTagDF = spark.sql(s"select count(*) from cat.mydb.customers.tag_history_tag")
    highlight("Tag history")
    histTagDF.show(10, false)
    spark.sql("alter table cat.mydb.customers drop tag history_tag")
    // create a branch, update it and merge it back to the main branch
    branchAndMerge("cat.mydb.customers")
    spark.sql("select * from cat.mydb.customers.refs").show()
    getSnapshotAsOfTime("cat.mydb.customers", LocalDateTime.parse("2023-05-15T23:54:43")).show()
    println("we are done")

  }

  private def getSnapshotAsOfTime(fullTableName: String, snaptime:LocalDateTime)
                                 (implicit sparkSession: SparkSession) = {
    val tableRef = Spark3Util.loadIcebergTable(sparkSession, fullTableName)
    val snapTimeMillis = snaptime.atOffset(ZoneOffset.UTC).toInstant.toEpochMilli
    val matchedSnapshot = Try(SnapshotUtil.snapshotIdAsOfTime(tableRef, snapTimeMillis))
    val snapshotID = matchedSnapshot.getOrElse(tableRef.currentSnapshot().snapshotId())
    sparkSession.sql(s"select * from $fullTableName VERSION AS OF $snapshotID").show()
    sparkSession.read
      .option("snapshot-id", snapshotID)
      .format("iceberg")
      .load(fullTableName)
      .withColumn("snapshot_id", lit(snapshotID))
  }

  private def branchAndMerge(fullTableName: String)(implicit sparkSession: SparkSession) = {

    sparkSession.sql(s"ALTER TABLE $fullTableName  DROP BRANCH if exists branch02 ")
    sparkSession.sql(s"select count(*) from $fullTableName").show()
    val tableRef = Spark3Util.loadIcebergTable(sparkSession, fullTableName)
    // create branch
    tableRef.manageSnapshots()
      .createBranch("branch02", tableRef.currentSnapshot().snapshotId())
      .commit()
    // read branch to df
    val branchDF = sparkSession.read
      .option("branch", "branch02")
      .table(fullTableName)
    // duplicate the data in the branch by writing to the branch
    branchDF
      .writeTo(s"$fullTableName.branch_branch02")
      .append()
    // view the branches
    sparkSession.sql(s"select * from $fullTableName.refs").show()
    sparkSession.sql(s"select count(*) from $fullTableName VERSION AS OF 'branch02'").show()

    sparkSession.sql(s"select count(*) from $fullTableName").show()
    // merging the branch
   tableRef.manageSnapshots()
      .fastForwardBranch(SnapshotRef.MAIN_BRANCH, "branch02")
      .commit()
    sparkSession.sql(s"select count(*) from $fullTableName").show()

  }

  private def updateTagOnCurrentTable(fullTableName: String, tagName:String, expiresAt:LocalDateTime)
                                     (implicit sparkSession: SparkSession): Unit = {
    val milisTilExpire = LocalDateTime.now().until(expiresAt, ChronoUnit.MILLIS)
    val tableRef = Spark3Util.loadIcebergTable(sparkSession, fullTableName)
    val currentSnapshot = tableRef.currentSnapshot().snapshotId()
      tableRef.manageSnapshots()
        .replaceTag(tagName, currentSnapshot)
        .setMaxRefAgeMs(tagName, milisTilExpire)
        .commit()
  }
  private def readDFFromTag(fullTableName: String, tagName: String)
                           (implicit sparkSession: SparkSession) = {
    sparkSession
      .read
      .option("tag", tagName)
      .table(fullTableName)
  }

  private def getCDCView(fullTableName: String, icebergCatalog: String, remove_carryovers:Boolean)(implicit sparkSession: SparkSession): DataFrame = {

    val milliNow = LocalDateTime.now().atOffset(ZoneOffset.UTC).toInstant.toEpochMilli
    val milliYest = LocalDateTime.now().minusYears(1).atOffset(ZoneOffset.UTC).toInstant.toEpochMilli

    sparkSession.sql(
      s"""
         |CALL $icebergCatalog.system.create_changelog_view(table => '$fullTableName',
         |changelog_view => 'cdcView',
         |remove_carryovers => $remove_carryovers,
         | options => map (
         |    'start-timestamp','$milliYest'
         |    ,'end-timestamp','$milliNow'
         |    )
         | )
         |""".stripMargin
    )
    sparkSession.sql("SELECT *  FROM cdcView")

  }

}