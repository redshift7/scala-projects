package com.example.spark

import java.text.SimpleDateFormat

object LandingToRawSCDJmtoms extends BaseJmtomsPipeline {
  SparkConfig.getSparkSession()
  private[this] val spark = SparkConfig.sparkSession

  private[this] val startDate = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").print(DateTime.now())
  private[this] val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  var config_appName, config_tableName, config_sourcePath, config_schemaPath, configPath, config_stagingPath, config_targetPath: String = _
  var config_startBatchNo, config_endBatchno: Any = _
  var config_scd1column, config_selectcols: Seq[String] = _
  var config_part_1, config_part_2, config_part_3: Column = _
  var config_part_1_str, config_part_2_str, config_part_3_str: String = _
  var epochnum, recordcount: Long = _
  var jsonSchema: StructType = _
  var listFile: Array[String] = _
  var config_additionalColumns, dfConfigFile: DataFrame = _
  epochnum = System.currentTimeMillis / 1000

  def readConfig(): Unit = {
    val basePathConfig = configInput
    configPath = basePathConfig + "config/default_config.json"

    LogManager.getRootLogger.info("JOB STARTED: Config => " + configPath)
    dfConfigFile = JSONToDF.getDF(configPath)
    val dfConfigJson = dfConfigFile.select($"rawpipeline.appname", $"rawpipeline.tablename", $"rawpipeline.sourcepath", $"rawpipeline.stagingpath", $"rawpipeline.targetpath", $"rawpipeline.startbatchno", $"rawpipeline.endbatchno", $"rawpipeline.scd1column", $"rawpipeline.selectcols", $"rawpipeline.schemafile")
    val dfConfigJsonRows = dfConfigJson.first()
    config_appName = dfConfigJsonRows.getString(0)
    config_tableName = dfConfigJsonRows.getString(1)
    config_sourcePath = dfConfigJsonRows.getString(2)
    config_stagingPath = dfConfigJsonRows.getString(3)
    config_targetPath = dfConfigJsonRows.getString(4)
    config_startBatchNo = dfConfigJsonRows.get(5)
    config_endBatchno = dfConfigJsonRows.get(6)
    config_scd1column = dfConfigJsonRows.getString(7).split(",").toSeq
    config_selectcols = dfConfigJsonRows.getString(8).split(",").toSeq
    config_schemaPath = dfConfigJsonRows.getString(9)
  }

  def load(): Unit = {
    config_additionalColumns = dfConfigFile.select($"rawpipeline.additional_columns")

    try {
      if (HdfsUtils.getLock(config_stagingPath + "/T_" + config_tableName)) {
        LogManager.getRootLogger.info("Reading schema file => " + config_schemaPath)
        var flag: Boolean = true
        jsonSchema = spark.read.json(config_schemaPath).schema
        val batchCount = 0
        if (batchCount <= 1) {
          listFile = listBatch(config_startBatchNo, config_endBatchno, config_sourcePath, config_tableName, config_appName)
          if (listFile.length == 0) {
            flag = false
          }
          if (flag) {
            processScd(config_startBatchNo, config_endBatchno, config_sourcePath, config_tableName, config_appName, listFile, jsonSchema, config_selectcols, config_additionalColumns, config_scd1column)
            LogManager.getRootLogger.info("Regular scd")
            updateStatus(listFile, startDate, recordcount)
          }
          else {
            LogManager.getRootLogger.error("JOB FAILED No Batch's to be processed ")
          }
        }
        synctoT0(config_appName, config_tableName, jsonSchema, config_targetPath)
        LogManager.getRootLogger.info("Cleanup Started")
        cleanUp()
        println("Releasing Lock file now")
      }
      else {
        LogManager.getRootLogger.info("Another schedule is still running")
        System.exit(0)
      }
    }
    finally {
      HdfsUtils.releaseLock(config_stagingPath + "/T_" + config_tableName + "/_LOCK")
      println("msck repair table raw.r_" + config_tableName)
      spark.sql("msck repair table raw.r_" + config_tableName + "")
    }
    spark.close()
    LogManager.getRootLogger.info("------------End---------------")
  }

  def doTransformations(): Unit = {}

  def save(): Unit = {}

  def synctoT0(config_appName: String, config_tableName: String, jsonSchema: StructType, config_targetPath: String): Int = {
    val synPath = config_targetPath + "/R_" + config_tableName + "/t=0"
    val epochList = config_targetPath + "/R_" + config_tableName

    var batchCount = 0
    var epochMax: DataFrame = null
    val firstRun = false

    if (!firstRun) {
      val hiveQuery = s"SELECT max(batch_no) FROM analytics.raw_app_sync_status where app_name='${config_appName.replace("'", "''")}' and table_name='${config_tableName.replace("'", "''")}' and activity_type='file_sync'"
      LogManager.getRootLogger.info("Reading hive sync status table")
      val hiveDf = spark.sql(hiveQuery).cache()
      var maxBatchno = hiveDf.first().getString(0)
      if (maxBatchno == null) {
        LogManager.getRootLogger.error("No batch's to be sync to 0 partition")
        maxBatchno = "0"
      }
      epochMax = spark.read.orc(epochList).select($"t").distinct().filter($"t" > maxBatchno)
      batchCount = epochMax.count().toInt
    }

    if (batchCount <= 1) {
      if (batchCount == 1) {
        val runBatch = epochMax.first().get(0)
        val readPath = epochList + "/t=" + runBatch
        LogManager.getRootLogger.info("Stage1: Deletion activity start " + DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").print(DateTime.now()))
        FileCopyHdfs.copyFolder(hdfs, readPath, synPath)

        val syncDf = spark.read.orc(readPath)

        if (config_tableName == "S_OMS_EXPSTORE_ORDER") {
          syncDf.write.partitionBy(config_part_1_str, config_part_2_str, config_part_3_str).mode(SaveMode.Append).orc(synPath)
        }
        else {
          syncDf.write.partitionBy(config_part_1_str).mode(SaveMode.Append).orc(synPath)
        }

        updateStatus(config_appName, config_tableName, runBatch, recordcount)
        LogManager.getRootLogger.info("Stage1: Deletion activity end " + DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").print(DateTime.now()))
      }
    }
    batchCount
  }

  def updateStatus(config_appName: String, config_tableName: String, runBatch: Any, count: Long): Unit = {
    val sync_time = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").print(DateTime.now())
    val tempdf = Seq(config_appName).toDF("app_name")
    val hiveDf = tempdf
      .withColumn("table_name", lit(config_tableName))
      .withColumn("batch_no", lit(runBatch))
      .withColumn("activity_type", lit("file_sync"))
      .withColumn("file_sync_time", lit(sync_time).cast("timestamp"))
      .withColumn("partition_sync_time", lit("").cast("timestamp"))
      .withColumn("file_delete_time", lit("").cast("timestamp"))
      .withColumn("record_count", lit(count))
    DFToHive.putDF("analytics.raw_app_sync_status", hiveDf)
    LogManager.getRootLogger.info("Successfully synced batch: " + runBatch)
  }

  def listBatch(config_startBatchNo: Any, config_endBatchno: Any, config_sourcePath: String, config_tableName: String, config_appName: String): Array[String] = {
    var listFile: Array[String] = null
    var flag: Boolean = true
    val status = hdfs.listStatus(new Path(config_sourcePath + "/L_" + config_tableName))

    if (config_startBatchNo != null) {
      if (isValidBatchFormat(config_startBatchNo.toString) == true) {
        if (config_endBatchno != null) {
          if (isValidBatchFormat(config_endBatchno.toString) == true) {
            listFile = status.map { x => x.getPath.toString.split("=").last }.filter(x => x >= config_startBatchNo.toString && x <= config_endBatchno.toString)
          }
          else {
            flag = false
            LogManager.getRootLogger.error("Invalid  batchno format for ENDBATCHNO")
          }
        }
        else {
          listFile = status.map { x => x.getPath.toString.split("=").last }.filter(x => x >= config_startBatchNo.toString)
        }
      }
      else {
        flag = false
        LogManager.getRootLogger.error("Invalid  batchno format for STARTBATCHNO")
      }
    }
    else {
      val hiveQuery = "SELECT max(batch_no) FROM analytics.raw_app_status where app_name='" + config_appName + "' and table_name='" + config_tableName + "'"
      LogManager.getRootLogger.info("Reading hive status table")
      val hiveDf = spark.sql(hiveQuery).cache()
      val maxBatchno = hiveDf.first().getString(0)
      if (maxBatchno == null) {
        LogManager.getRootLogger.debug("Raw_App_Status does not contain any previous run records for table: " + config_tableName)
        LogManager.getRootLogger.debug("Recommended Initial SCD1 RUN by specifying STARTBATCHNO or ENDBATCHNO in config file ")
        flag = false
      }
      else {
        listFile = status.map { x => x.getPath.toString.split("=").last }.filter { x => x > maxBatchno }
      }
    }

    listFile
  }

  def processScd(config_startBatchNo: Any, config_endBatchno: Any, config_sourcePath: String, config_tableName: String, config_appName: String, listFile: Array[String], jsonSchema: StructType, config_selectcols: Seq[String], config_additionalColumns: DataFrame, config_scd1column: Seq[String]): Array[Row] = {
    var dFDelta, dFDistinct, joinDf, dfcast: DataFrame = null
    var castcols: Seq[String] = null
    var Datedistinct: Array[Any] = null
    var dfExplodeArray: DataFrame = null
    val startDate = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").print(DateTime.now())
    val synPath = config_targetPath + "/R_" + config_tableName + "/t=0"
    println("Main Path:" + synPath)
    if (listFile.length != 0) {
      for (file <- listFile) {
        println("Files: " + config_sourcePath + "/L_" + config_tableName + "/batchno=" + file)
        val dfjsonfile = spark.read.schema(jsonSchema).option("wholefile", "true").json(config_sourcePath + "/L_" + config_tableName + "/batchno=" + file)
        val dFfile = dfjsonfile.select(config_selectcols.map(name => col(name)): _*)
        val dfaddedcol = dFfile.withColumn("kafka_ts", $"current_ts").withColumn("trail_file_pos", $"pos").withColumn("last_updated_date", lit(startDate)).drop("current_ts", "pos")
        if (dfcast != null) {
          dfcast = dfcast.union(dfaddedcol)
        }
        else {
          dfcast = dfaddedcol
        }
      }
    }

    val dfRank = dfcast.withColumn("rank", row_number().over(Window.partitionBy(config_scd1column.map(col(_)): _*).orderBy($"kafka_ts".desc))).filter($"rank" === 1).drop("rank")
    castcols = dfRank.columns
    println("Casting columns to string datatype")
    dFDelta = withColumnCast(dfRank, castcols, "string")

    LogManager.getRootLogger.info("Adding columns with rowkey and f_key")
    if (config_additionalColumns.head(1).nonEmpty) {
      dfExplodeArray = config_additionalColumns.select(explode($"additional_columns")).select($"col.columnkey", $"col.columnname", $"col.columnformula")
      dfExplodeArray.collect().foreach { row =>
        row.toSeq
        if (row.getString(0) != null && row.getString(1) != null) {
          row.getString(0) match {
            case "row_key" | "f_key" =>
              dFDelta = dFDelta.withColumn(row.getString(1), concat_ws("", row.getString(2).split(",").map(c => col(c)): _*))
              dFDelta = dFDelta.withColumn(row.getString(1), regexp_replace(col(row.getString(1)), "[-:.,/ ]", ""))
            case "part_1" =>
              if (config_tableName == "S_OMS_EXPSTORE_ORDER") {
                dFDelta = dFDelta.withColumn(row.getString(1), year(to_date(col(row.getString(2)))))
              }
              else {
                dFDelta = dFDelta.withColumn(row.getString(1), substring(col(row.getString(2)), 0, 3))
              }
              config_part_1 = col(row.getString(1))
              config_part_1_str = row.getString(1)
            case "part_2" =>
              dFDelta = dFDelta.withColumn(row.getString(1), month(to_date(col(row.getString(2)))))
              config_part_2 = col(row.getString(1))
              config_part_2_str = row.getString(1)
            case "part_3" =>
              dFDelta = dFDelta.withColumn(row.getString(1), to_date(col(row.getString(2))))
              config_part_3 = col(row.getString(1))
              config_part_3_str = row.getString(1)
            case _ => None
          }
        }
        else {
          LogManager.getRootLogger.info("No Additional columns found")
        }
      }
    }
    else {
      LogManager.getRootLogger.info("No Additional columns node in config file:")
    }

    LogManager.getRootLogger.info("Getting distinct partition for running scd1 logic")
    dFDelta.show(5, false)
    if (config_tableName == "S_OMS_EXPSTORE_ORDER") {
      dFDistinct = dFDelta.select(config_part_1.cast("int"), config_part_2.cast("int"), config_part_3).distinct()
    }
    else {
      dFDistinct = dFDelta.select(config_part_1).distinct()
    }
    val Rowdistinct = dFDistinct.collect()
    if (config_tableName == "S_OMS_EXPSTORE_ORDER") {
      Datedistinct = Rowdistinct.map(x => x.get(2))
    }
    else {
      Datedistinct = Rowdistinct.map(x => x.get(0))
    }
    val rawDf = spark.read.orc(synPath)
    val cols = dFDelta.columns
    if (config_tableName == "S_OMS_EXPSTORE_ORDER") {
      joinDf = rawDf.filter(rawDf(config_part_3_str).isin(Datedistinct: _*)).select(cols.head, cols.tail: _*)
    }
    else {
      joinDf = rawDf.filter(rawDf(config_part_1_str).isin(Datedistinct: _*)).select(cols.head, cols.tail: _*)
    }
    val dfLanding = dFDelta.union(joinDf)
    val dFScd1 = dfLanding.withColumn("rank", row_number().over(Window.partitionBy(config_scd1column.map(col(_)): _*).orderBy($"trail_file_pos".desc))).filter($"rank" === 1).drop("rank")
    LogManager.getRootLogger.info("No of Partitions: " + dFScd1.rdd.partitions.length)
    dFScd1.printSchema()
    val writePath = config_targetPath + "/R_" + config_tableName + "/t=" + epochnum
    println("writing to:" + writePath)
    recordcount = dFScd1.count()
    if (config_tableName == "S_OMS_EXPSTORE_ORDER") {
      dFScd1.write.partitionBy(config_part_1_str, config_part_2_str, config_part_3_str).mode(SaveMode.Append).orc(writePath)
    }
    else {
      dFScd1.write.partitionBy(config_part_1_str).mode(SaveMode.Append).orc(writePath)
    }
    LogManager.getRootLogger.info("Stage2: After writing activity  " + DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").print(DateTime.now()))
    Rowdistinct
  }

  def updateStatus(listFile: Array[String], startDate: String, count: Long): Unit = {
    LogManager.getRootLogger.info("Stage3:Update hive table activity end  " + DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").print(DateTime.now()))
    val endDate = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").print(DateTime.now())
    LogManager.getRootLogger.info("Inserting into hive table")
    for (batch_nos <- listFile) {
      val tempdf = Seq(config_appName).toDF("app_name")
      val statsDf = tempdf.withColumn("table_name", lit(config_tableName)).withColumn("batch_no", lit(batch_nos)).withColumn("process_start_time", lit(startDate).cast("timestamp")).withColumn("process_end_time", lit(endDate).cast("timestamp")).withColumn("record_count", lit(count))
      DFToHive.putDF("analytics.raw_app_status", statsDf)
    }
  }

  def cleanUp(): Unit = {
    val file_delete_Date = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").print(DateTime.now())
    val hiveQuery = "select batch_no from (SELECT batch_no, sum(case when activity_type='file_sync' then 1 else 0  end) file_sync,sum(case when activity_type='partition_sync' then 1 else 0  end) part_sync ,sum(case when activity_type='batch_deleted' then 1 else 0  end) batch_del  FROM analytics.raw_app_sync_status where app_name='" + config_appName + "' and table_name='" + config_tableName + "' group by batch_no) where file_sync>0 and batch_del=0"

    LogManager.getRootLogger.info("Reading hive sync status table")
    println(hiveQuery)
    val cleanupDf = spark.sql(hiveQuery).cache()
    val df = cleanupDf.collect()
    df.map { x => {
      if (x.getString(0) != "0") {
        println(x.getString(0))
        val deletePath = config_targetPath + "/R_" + config_tableName + "/t=" + x.getString(0)
        if (HdfsUtils.existPath(deletePath)) {
          println(deletePath)
          HdfsUtils.delete(deletePath)
          val tempdf = Seq(config_appName).toDF("app_name")
          val hiveDf = tempdf
            .withColumn("table_name", lit(config_tableName))
            .withColumn("batch_no", lit(x.getString(0)))
            .withColumn("activity_type", lit("batch_deleted"))
            .withColumn("file_sync_time", lit("").cast("timestamp"))
            .withColumn("partition_sync_time", lit("").cast("timestamp"))
            .withColumn("file_delete_time", lit(file_delete_Date).cast("timestamp"))
            .withColumn("record_count", lit("").cast("timestamp"))
          DFToHive.putDF("analytics.raw_app_sync_status", hiveDf)
        }
        x
      }
    }
    }
  }

  def isValidBatchFormat(value: String): Any = {
    try {
      val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
      val date = sdf.parse(value)
      if (value.equals(sdf.format(date))) {
        true
      }
    }
    catch {
      case e: Exception => false
    }
  }

  def withColumnCast(castDataframe: DataFrame, castColumns: Seq[String], castDataType: String): DataFrame = {
    var castDf = castDataframe
    for (i <- 0 to castColumns.length - 1) {
      castDf = castDf.withColumn(castColumns(i), when(col(castColumns(i)).isNotNull, col(castColumns(i)).cast(castDataType)).otherwise(null))
    }
    castDf
  }
}
