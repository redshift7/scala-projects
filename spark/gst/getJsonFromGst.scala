package com.example.spark.gst

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import scala.collection.mutable.{ArrayBuffer, Queue}
import scala.io._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j.LogManager
import com.example.commons.config.SparkConfig
import com.example.commons.lang.{CSVToDF, DFToCSV, JSONToDF}
import com.example.commons.util.{DateUtils, HdfsUtils}
import com.example.gst.v1.BaseGSTProcessBlock
import scalaj.http.Http
import com.fasterxml.jackson.databind.ObjectMapper
import scala.util.{Try, Success, Failure}

case class Action(action_id: String, timestamp: String, action_type: String, data: String)
case class GSTApiResponse(status: String, message: String, data: Option[String], timestamp: String)

object getJsonFromGst extends BaseGSTProcessBlock {
  val json_data = new ArrayBuffer[Action]
  val no_of_part = 10
  SparkConfig.getSparkSession()
  private[this] val spark = SparkConfig.sparkSession

  val part_queue = 5
  val retry_limit = 5
  val api_timeout = 100000
  val mapper = new ObjectMapper()

  var retryRDD: scala.collection.mutable.Queue[Action] = scala.collection.mutable.Queue.empty[Action]
  val queue_size = 3
  var ac: Action = _
  var r = 0
  var queue_start, year: Long = 0
  var Fp, appkey, authtoken, sek, writePath, month, rootPath, gst_type, lockPath, Section, fy, statusPath, sessionPath, config_fy, logPath, log, start_hour, end_hour, config_minute, filter_section, schedule, configPath, check: String = ""
  var api_url, api_key, api_secret: String = ""
  var filterDf, dfConfigSuccess, lastMonthsDf: DataFrame = _
  var cal: Calendar = Calendar.getInstance
  var totalRecordCount: Long = 0L
  var processedRecordCount: Long = 0L
  var apiCallCount: Long = 0L
  var apiSuccessCount: Long = 0L
  var apiFailureCount: Long = 0L

  def readConfig(): Unit = {
    println("--------------------------Starting---------------------")
    val basePathConfig: String = inputPath
    filter_section = section
    schedule = runtype
    val lastDate = cal.get(Calendar.DATE)
    val curr_day = cal.get(Calendar.DAY_OF_WEEK)
    val e1 = new SimpleDateFormat("EEEE")
    if (basePathConfig.contains("gstr2a") && (lastDate == 12 || lastDate == 13)) {
      configPath = basePathConfig + "config/default_config_24hr.json"
      LogManager.getRootLogger.info("JOB STARTED with 24HRs refresh time: Config => " + configPath)
      check = "24hr"
    }
    else if (basePathConfig.contains("gstr2a") && (curr_day == 7 || curr_day == 1)) {
      configPath = basePathConfig + "config/default_config_1day.json"
      LogManager.getRootLogger.info("JOB STARTED for previous year refresh,Day:" + e1.format(cal.getTime).toUpperCase + ", Config => " + configPath)
    }
    else {
      configPath = basePathConfig + "config/default_config.json"
      LogManager.getRootLogger.info("JOB STARTED: Config => " + configPath)
    }

    val dfConfigFile = JSONToDF.getDF(configPath)
    val dfConfigJson = dfConfigFile.select($"config.rootpath", $"config.metapath", $"config.sessionpath", $"config.datapath", $"config.statuspath", $"config.validpath",
      $"config.source_config_path", $"config.flatprocesspath", explode($"config.schedule"))
      .select($"rootpath", $"col.gst_type", $"metapath", $"sessionpath", $"datapath", $"statuspath", $"validpath", $"source_config_path", $"col.schedule_id",
        $"col.fy", $"col.force_refresh_month", $"col.refresh_timeout_hours", $"flatprocesspath")
    dfConfigSuccess = dfConfigFile.select($"config.manual_success.start_hour", $"config.manual_success.end_hour", $"config.manual_success.minute")
    
    api_url = dfConfigFile.select($"config.api.url").first().getString(0)
    api_key = dfConfigFile.select($"config.api.key").first().getString(0)
    api_secret = dfConfigFile.select($"config.api.secret").first().getString(0)
    
    LogManager.getRootLogger.info("API Configuration loaded: " + api_url)
    filterDf = dfConfigJson.filter($"schedule_id" === schedule)
  }

  def callGSTApi(gstin: String, fy: String, section: String, retryCount: Int = 0): Try[String] = {
    val timestamp = DateUtils.getDate()
    apiCallCount = apiCallCount + 1
    
    try {
      val endpoint = api_url + "/fetch?gstin=" + gstin + "&fy=" + fy + "&section=" + section
      LogManager.getRootLogger.info("Calling GST API: " + endpoint)
      
      val headers = Map(
        "Content-Type" -> "application/json",
        "Authorization" -> ("Bearer " + api_key),
        "X-API-Key" -> api_key,
        "X-API-Secret" -> api_secret,
        "X-Timestamp" -> timestamp
      )
      
      val response = Http(endpoint)
        .headers(headers)
        .timeout(api_timeout, api_timeout)
        .asString
      
      if (response.code == 200) {
        apiSuccessCount = apiSuccessCount + 1
        LogManager.getRootLogger.info("GST API call successful for GSTIN: " + gstin + " Response code: " + response.code)
        Success(response.body)
      } else if (response.code == 429 || response.code == 503) {
        if (retryCount < retry_limit) {
          LogManager.getRootLogger.warn("GST API rate limited. Retry " + (retryCount + 1) + " of " + retry_limit)
          Thread.sleep(2000 * (retryCount + 1))
          callGSTApi(gstin, fy, section, retryCount + 1)
        } else {
          apiFailureCount = apiFailureCount + 1
          Failure(new Exception("GST API rate limited after " + retry_limit + " retries"))
        }
      } else if (response.code >= 400 && response.code < 500) {
        apiFailureCount = apiFailureCount + 1
        Failure(new Exception("GST API client error: " + response.code + " " + response.body))
      } else {
        apiFailureCount = apiFailureCount + 1
        Failure(new Exception("GST API server error: " + response.code))
      }
    } catch {
      case e: Exception =>
        if (retryCount < retry_limit) {
          LogManager.getRootLogger.warn("GST API call failed, retrying: " + e.getMessage)
          Thread.sleep(1000 * (retryCount + 1))
          callGSTApi(gstin, fy, section, retryCount + 1)
        } else {
          apiFailureCount = apiFailureCount + 1
          Failure(e)
        }
    }
  }

  def parseGSTApiResponse(responseJson: String): Try[DataFrame] = {
    try {
      val response = mapper.readValue(responseJson, classOf[java.util.Map[String, Any]])
      
      if (response.containsKey("status") && response.get("status").toString == "success") {
        if (response.containsKey("data")) {
          val dataStr = response.get("data").toString
          val dfFromJson = spark.read.json(spark.sparkContext.parallelize(Seq(dataStr)))
          LogManager.getRootLogger.info("Successfully parsed GST API response")
          Success(dfFromJson)
        } else {
          Failure(new Exception("No data field in API response"))
        }
      } else {
        val message = response.getOrDefault("message", "Unknown error").toString
        Failure(new Exception("GST API error: " + message))
      }
    } catch {
      case e: Exception =>
        LogManager.getRootLogger.error("Failed to parse GST API response: " + e.getMessage)
        Failure(e)
    }
  }

  def fetchGSTDataFromApi(gstin: String, fy: String, section: String): Option[DataFrame] = {
    LogManager.getRootLogger.info("Fetching GST data from API for GSTIN: " + gstin)
    
    callGSTApi(gstin, fy, section) match {
      case Success(responseBody) =>
        parseGSTApiResponse(responseBody) match {
          case Success(df) =>
            LogManager.getRootLogger.info("Successfully fetched and parsed GST data for GSTIN: " + gstin)
            Some(df)
          case Failure(e) =>
            LogManager.getRootLogger.error("Failed to parse GST API response: " + e.getMessage)
            retryRDD.enqueue(Action("api_parse_" + System.currentTimeMillis(), DateUtils.getDate(), "API_PARSE_ERROR", gstin))
            None
        }
      case Failure(e) =>
        LogManager.getRootLogger.error("Failed to call GST API: " + e.getMessage)
        retryRDD.enqueue(Action("api_call_" + System.currentTimeMillis(), DateUtils.getDate(), "API_CALL_ERROR", gstin))
        None
    }
  }

  def doTransformations(): Unit = {
    filterDf.collect().foreach { row =>
      row.toSeq
      println("------------started------------")
      log = ""
      var RDD = ArrayBuffer[Action]()
      rootPath = row.getString(0)
      gst_type = row.getString(1)
      writePath = rootPath + row.getString(4) + gst_type + "/fy=" + row.getString(9)
      statusPath = rootPath + row.getString(5) + gst_type + "/fy=" + row.getString(9) + "/"
      logPath = rootPath + row.getString(4) + "temp/log/" + gst_type + "/" + DateUtils.getDate() + ".log"
      lockPath = rootPath + row.getString(4)

      sessionPath = rootPath + row.getString(3)
      config_fy = row.getString(9)
      val config_force_refresh_month = row.getString(10)
      val config_refresh_timeout_hours = row.getString(11)

      println("Job started--> Download-> fy-" + config_fy + " gst-type: " + gst_type.toUpperCase + " section: " + filter_section.toUpperCase())
      val validtimeStamp = com.example.commons.util.DateUtils.genTimestamp(config_refresh_timeout_hours.toInt)
      
      if (check == "24hr") {
        lastMonthsDf = com.example.commons.util.DateUtils.genLastMonths(config_force_refresh_month.toInt).tail.toDF()
        LogManager.getRootLogger.info("Months => " + lastMonthsDf.show(false))
      }
      else {
        lastMonthsDf = com.example.commons.util.DateUtils.genLastMonths(config_force_refresh_month.toInt).toDF()
        LogManager.getRootLogger.info("No. of months =" + lastMonthsDf.count)
      }

      val manual_success = dfConfigSuccess.first()
      start_hour = manual_success.getString(0)
      end_hour = manual_success.getString(1)
      config_minute = manual_success.getString(2)

      try {
        val dfStatus = CSVToDF.getDF(statusPath + gst_type + "_" + "FileStatus_" + filter_section + "_" + config_fy + ".csv")
        val dfSession = spark.read.option("inferSchema", "true").csv(sessionPath + "ValidSessionLog.csv").toDF("COMPANY1", "BUSSINESSPALCE1", "GSTIN1", "FP1", "SECTION1", "USER1", "APPKEY", "SEK", "AUTHTOKEN", "TEMP")

        LogManager.getRootLogger.info("Processing GST data for period: " + config_fy)
        
        var combinedDf: DataFrame = null
        dfSession.collect().foreach { sessionRow =>
          val gstin = sessionRow.getAs[String]("GSTIN1")
          val section = sessionRow.getAs[String]("SECTION1")
          
          LogManager.getRootLogger.info("Fetching data for GSTIN: " + gstin + ", Section: " + section)
          fetchGSTDataFromApi(gstin, config_fy, section) match {
            case Some(apiDf) =>
              if (combinedDf == null) {
                combinedDf = apiDf
              } else {
                combinedDf = combinedDf.union(apiDf)
              }
            case None =>
              LogManager.getRootLogger.warn("Failed to fetch data for GSTIN: " + gstin)
          }
        }
        
        if (combinedDf != null) {
          processGstJsonData(combinedDf, dfStatus, config_fy, gst_type, writePath, statusPath)
          LogManager.getRootLogger.info("GST transformation completed successfully for " + gst_type)
        } else {
          LogManager.getRootLogger.error("No data retrieved from GST API")
        }
      } catch {
        case e: Exception =>
          LogManager.getRootLogger.error("Error during GST transformation: " + e.getMessage)
          log = log + "ERROR: " + e.getMessage
          retryRDD.enqueue(Action("transform_" + System.currentTimeMillis(), DateUtils.getDate(), "TRANSFORM_ERROR", gst_type))
      }
    }
  }

  private def processGstJsonData(dfApiData: DataFrame, dfStatus: DataFrame, fy: String, gstType: String, outputPath: String, statusPath: String): Unit = {
    try {
      val joinedDf = dfApiData.join(dfStatus, dfApiData("gstin") === dfStatus("gstin"), "inner")
        .select(dfApiData("*"))
        .filter($"status" === "VALID" || $"status" === "ACTIVE")

      val validRecords = joinedDf.count()
      totalRecordCount = totalRecordCount + validRecords
      processedRecordCount = processedRecordCount + validRecords

      LogManager.getRootLogger.info("Found " + validRecords + " valid records for GST Type: " + gstType)

      joinedDf.write.mode("overwrite").json(outputPath)
      LogManager.getRootLogger.info("Successfully wrote " + validRecords + " records to " + outputPath)

      val summaryDf = Seq((gstType, fy, validRecords, DateUtils.getDate())).toDF("gst_type", "financial_year", "record_count", "process_date")
      summaryDf.write.mode("append").csv(statusPath + "gst_summary.csv")
      
    } catch {
      case e: Exception =>
        LogManager.getRootLogger.error("Failed to process GST JSON data: " + e.getMessage)
        throw e
    }
  }

  def retryFailedRecords(): Unit = {
    LogManager.getRootLogger.info("Starting retry for " + retryRDD.length + " failed records")
    while (retryRDD.nonEmpty && r < retry_limit) {
      val action = retryRDD.dequeue()
      try {
        println("Retrying action: " + action.action_id + " of type: " + action.action_type)
        action.action_type match {
          case "API_CALL_ERROR" =>
            fetchGSTDataFromApi(action.data, config_fy, filter_section)
          case "TRANSFORM_ERROR" =>
            LogManager.getRootLogger.info("Retrying transformation for: " + action.data)
          case _ =>
            LogManager.getRootLogger.info("Unknown action type: " + action.action_type)
        }
        r = r + 1
      } catch {
        case e: Exception =>
          LogManager.getRootLogger.warn("Retry attempt " + r + " failed: " + e.getMessage)
          if (r < retry_limit) {
            retryRDD.enqueue(action)
          }
      }
    }
    if (retryRDD.nonEmpty) {
      LogManager.getRootLogger.error("Failed records after " + retry_limit + " retries: " + retryRDD.length)
    }
  }

  def validateData(dfInput: DataFrame): Boolean = {
    try {
      if (dfInput == null || dfInput.count() == 0) {
        LogManager.getRootLogger.warn("Validation failed: Empty or null DataFrame")
        return false
      }
      val requiredCols = Seq("GSTIN1", "COMPANY1", "USER1")
      val missingCols = requiredCols.filter(!dfInput.columns.contains(_))
      if (missingCols.nonEmpty) {
        LogManager.getRootLogger.error("Missing required columns: " + missingCols.mkString(", "))
        return false
      }
      true
    } catch {
      case e: Exception =>
        LogManager.getRootLogger.error("Validation error: " + e.getMessage)
        false
    }
  }

  def load(): Unit = {
    println("Loading GST data")
    try {
      readConfig()
      LogManager.getRootLogger.info("Configuration loaded successfully")
      doTransformations()
      if (retryRDD.nonEmpty) {
        retryFailedRecords()
      }
    } catch {
      case e: Exception =>
        LogManager.getRootLogger.error("Failed to load GST data: " + e.getMessage)
        throw e
    }
  }

  def save(): Unit = {
    println("Saving GST data")
    try {
      LogManager.getRootLogger.info("Starting GST data save process")
      LogManager.getRootLogger.info("Total records processed: " + totalRecordCount)
      LogManager.getRootLogger.info("Successfully saved records: " + processedRecordCount)
      LogManager.getRootLogger.info("API call statistics - Total: " + apiCallCount + ", Success: " + apiSuccessCount + ", Failures: " + apiFailureCount)
      LogManager.getRootLogger.info("GST data saved successfully")
    } catch {
      case e: Exception =>
        LogManager.getRootLogger.error("Failed to save GST data: " + e.getMessage)
        throw e
    }
  }

  def execute(): Unit = {
    try {
      LogManager.getRootLogger.info("GST JSON Processing Job Started")
      load()
      save()
      LogManager.getRootLogger.info("GST JSON Processing Job Completed Successfully")
    } catch {
      case e: Exception =>
        LogManager.getRootLogger.error("GST JSON Processing Job Failed: " + e.getMessage)
        throw e
    }
  }
}
