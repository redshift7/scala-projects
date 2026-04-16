package com.example.spark

import java.util.Properties
import java.util.logging.LogManager

object TrustedToExternalCOD extends BaseExternalPipelineBlocks {

  SparkConfig.getSparkSession()
  val spark: SparkSession = SparkConfig.sparkSession
  val logManager = LogManager.getRootLogger

  LogManager.getRootLogger.info("--------------Starting--------------")
  var headers, params: Map[String, String] = _
  var basePathConfig, configPath, schemaFilePath: String = null
  var record_count, totalCount: Long = 0L;
  var config_errortableName, maxRecordDate, config_table_name, hiveQuery, config_fromDatetime, source_system, config_sourcecred_path, jdbcUrl, config_header2, config_param2, config_baseQuery, config_errorTablePath, config_batchSize, config_statusPath, config_mailTo, config_mailFrom, config_param1, config_header1, config_data_type, config_authname, config_authpass, authcomputed, config_url, config_col1, config_col2, config_headers, apiv, config_sourcepath, config_app_name, config_targetpath, endDate1, config_rtype, values: String = null
  var config_additionalColumns, jsondata_arg, jsondata_allow, jsondata_block, data: DataFrame = null
  val CONST_MAX_BATCH_NO: String = DateTimeFormat.forPattern("yyyyMMddHH").print(DateTime.now())
  val startDate: String = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").print(DateTime.now())
  val record_date: String = DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now())
  val batch_no: String = DateTimeFormat.forPattern("yyyyMMddHHmmss").print(DateTime.now())
  var connectionProperties: Properties = null


  def readConfig(): Unit = {
    basePathConfig = baseConfigInputPath
    apiv = arg
    configPath = basePathConfig + "config/default_config.json"
    if (apiv == "DEV" || apiv == "PREPRD") {
      source_system = "HDV"
    }
    else if (apiv == "PRD") {
      source_system = "HANA"
    }
    logManager.info("Using variables: " + source_system)
    val dfConfigFile = JSONToDF.getDF(configPath)
    val queryFilePath = basePathConfig + "query/default_query.txt"
    val queryFileContent = spark.read.textFile(queryFilePath)
    config_baseQuery = queryFileContent.first().mkString
    config_additionalColumns = dfConfigFile.select("externalpipeline")
    config_app_name = config_additionalColumns.select("externalpipeline.app_name").first().getString(0)
    config_table_name = config_additionalColumns.select("externalpipeline.table_name").first().getString(0)
    config_statusPath = config_additionalColumns.select("externalpipeline.statuspath").first().getString(0)
    config_header1 = config_additionalColumns.select("externalpipeline.header1").first().getString(0)
    config_header2 = config_additionalColumns.select("externalpipeline.header2").first().getString(0)
    config_param1 = config_additionalColumns.select("externalpipeline.param1").first().getString(0)
    config_param2 = config_additionalColumns.select("externalpipeline.param2").first().getString(0)
    config_authname = config_additionalColumns.select("externalpipeline.api." + apiv + ".apiKey").first().getString(0)
    config_authpass = config_additionalColumns.select("externalpipeline.api." + apiv + ".apiPass").first().getString(0)
    config_url = config_additionalColumns.select("externalpipeline.api." + apiv + ".url").first().getString(0)
    config_errorTablePath = config_additionalColumns.select("externalpipeline.errorTable.path").first().getString(0)
    config_errortableName = config_additionalColumns.select("externalpipeline.errorTable.name").first().getString(0)
    config_mailFrom = config_additionalColumns.select("externalpipeline.mail.from").first().getString(0)
    config_mailTo = config_additionalColumns.select("externalpipeline.mail.to").first().getString(0)
    config_sourcecred_path = config_additionalColumns.select("externalpipeline.sourceconfig").first().getString(0)
    config_fromDatetime = config_additionalColumns.select("externalpipeline.fromdate").first().getString(0)
  }

  def read(): Unit = {
    if (config_fromDatetime != null) {
      config_fromDatetime = config_fromDatetime.toString
      if (isValidDateTime(config_fromDatetime) != false) {
        maxRecordDate = config_fromDatetime
        logManager.info("maxRecordDate is " + maxRecordDate)
      }
      else {
        logManager.error("Invalid Datetime format for FROMDATETIME")
        throw new Exception("Invalid Datetime format for FROMDATETIME")
      }
    }
    else {
      val escapedAppName = config_app_name.replace("'", "''")
      val escapedTableName = config_table_name.replace("'", "''")
      val hiveQuery = s"SELECT max(batch_no) FROM analytics.raw_app_status where app_name='$escapedAppName' and table_name='$escapedTableName'"
      maxRecordDate = spark.sql(hiveQuery).first().getString(0)
      if (maxRecordDate == null) {
        logManager.debug("App_Status does not contain any previous run for table: " + config_table_name)
        logManager.debug("Recommended Initial Load by specifying FROMDATETIME in config file " + configPath)
        logManager.debug("checking FROMDATETIME in config file " + configPath)
      }
      else {
        logManager.info("maxRecordDate is " + maxRecordDate)
      }
    }
    val dfSourceConfig = JSONToDF.getDF(config_sourcecred_path)
    val dfSourceJson = dfSourceConfig.select("source_system." + source_system)
    jdbcUrl = dfSourceJson.select(source_system + ".url").first().getString(0)
    connectionProperties = new java.util.Properties
    connectionProperties.setProperty("user", dfSourceJson.select(source_system + ".user").first().getString(0))
    connectionProperties.setProperty("password", dfSourceJson.select(source_system + ".password").first().getString(0))
    connectionProperties.setProperty("driver", dfSourceJson.select(source_system + ".driver").first().getString(0))
    var sourceQueryHeader = "(" + config_baseQuery + ")temp_alias"
    sourceQueryHeader = sourceQueryHeader.replaceAll("dummy_date", maxRecordDate)
    logManager.info("Query used : " + sourceQueryHeader)
    jsondata_arg = spark.read.jdbc(url = jdbcUrl, table = sourceQueryHeader, properties = connectionProperties)
    totalCount = jsondata_arg.cache.count()
  }

  def preSummerisation(): Unit = {}

  def doTransformations(): Unit = {
    headers = Map(config_header1 -> config_authname, config_header2 -> config_authpass)
    jsondata_allow = jsondata_arg.filter($"COD_ALLOWED" === 1).cache()
    jsondata_block = jsondata_arg.filter($"COD_ALLOWED" === 0).cache()
    val record_count_a = jsondata_allow.count
    val record_count_b = jsondata_block.count

    if (record_count_a > 0) {
      batchPrep(jsondata_allow, record_count_a, "allow")
    }
    else {
      logManager.info("No ids found for allow")
    }
    if (record_count_b > 0) {
      batchPrep(jsondata_block, record_count_b, "block")
    }
    else {
      logManager.info("No ids found for block")
    }
  }

  def postSummerisation(): Unit = {
    logManager.info("---------------ENDING---------------")
  }

  def save(): Unit = {
    if (totalCount > 0) {
      val endDate = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").print(DateTime.now())
      val tempdf = Seq(config_app_name).toDF("app_name")
      val statsDf = tempdf.withColumn("table_name", lit(config_table_name)).withColumn("batch_no", lit(batch_no)).withColumn("process_start_time", lit(startDate)).withColumn("process_end_time", lit(endDate)).withColumn("record_count", lit(totalCount))
      DFToHive.putDF("analytics.raw_app_status", statsDf)
      logManager.info("JOB SUCCEED: Record count is " + totalCount + " for table: " + config_app_name + "." + config_table_name)
    }
    spark.sql("msck repair table analytics.api_app_status")
    spark.close()
  }

  def isValidDateTime(dateTime: String) = {
    val format = "yyyyMMddHHmmss"
    try {
      java.time.LocalDate.parse(dateTime, java.time.format.DateTimeFormatter.ofPattern(format))
    }
    catch {
      case e: Exception =>
        false
    }
  }

  def batchPrep(data: DataFrame, record_count: Long, operation: String) = {
    val batchSize = 700
    val custIds = data.select($"CUST_ID").rdd.map(r => r(0)).collect().toList
    val numBatches = Math.ceil(custIds.size.toDouble / batchSize.toDouble).toInt
    for (i <- 0 until numBatches) {
      val start = i * batchSize
      val end = Math.min(start + batchSize, custIds.size)
      val batchIds = custIds.slice(start, end)
      val id_list = batchIds.mkString(",")
      val batchParams = Map(config_param1 -> id_list, config_param2 -> operation)
      val batchResponse = apiutils.apipush(config_url, headers, batchParams)
      apiutils.responsestore(config_app_name, config_statusPath, config_errorTablePath, batchResponse, record_count, operation, apiv)
    }
  }

}
