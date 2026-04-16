package com.example.spark.gst

import com.example.commons.config.SparkConfig
import com.example.commons.lang.{CSVToDF, DFToCSV, JSONToDF}
import com.example.commons.util.{DateUtils, HdfsUtils}
import com.example.gst.v1.BaseGSTProcessBlock
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import java.util.Properties
import scala.collection.mutable.ArrayBuffer

object GenerateFlat extends BaseGSTProcessBlock {
  SparkConfig.getSparkSession()
  private[this] val spark = SparkConfig.sparkSession
  private[this] val current_timestamp_dt = com.example.commons.util.DateUtils.getDate()

  import spark.implicits._

  var baseSchemaPth, filter_section, rootPath, config_sourcecred_path, jdbcUrl, user, pass, finalfp: String = _
  var configPath: String = _
  var prop: Properties = _
  var filterDf, perioddf, finaldf, dfConfigFile: DataFrame = _
  var recordCount: Long = 0L
  var fileCount: Int = 0
  var errorCount: Int = 0
  var startDate: String = _
  var endDate: String = _
  var partitionColList: ArrayBuffer[String] = ArrayBuffer()

  def readConfig(): Unit = {
    println("-----------------------Starting-----------------------")
    val basePathConfig = inputPath
    filter_section = section
    val schedule = runtype
    configPath = basePathConfig + "config/default_config.json"
    LogManager.getRootLogger.info("JOB STARTED: Config => " + configPath)
    
    try {
      dfConfigFile = JSONToDF.getDF(configPath)
      rootPath = dfConfigFile.select($"config.rootpath").first().getString(0)
      baseSchemaPth = dfConfigFile.select($"config.schemapath").first().getString(0)
      config_sourcecred_path = dfConfigFile.select($"config.source_config_path").first().getString(0)
      jdbcUrl = dfConfigFile.select($"config.jdbc.url").first().getString(0)
      user = dfConfigFile.select($"config.jdbc.user").first().getString(0)
      pass = dfConfigFile.select($"config.jdbc.password").first().getString(0)
      
      LogManager.getRootLogger.info("Configuration loaded successfully")
      LogManager.getRootLogger.info("Root Path: " + rootPath)
      LogManager.getRootLogger.info("Schema Path: " + baseSchemaPth)
    } catch {
      case e: Exception =>
        LogManager.getRootLogger.error("Failed to read configuration: " + e.getMessage)
        throw e
    }
  }

  def validateSchema(df: DataFrame): Boolean = {
    try {
      if (df == null || df.count() == 0) {
        LogManager.getRootLogger.warn("Schema validation failed: Empty DataFrame")
        return false
      }
      LogManager.getRootLogger.info("Schema validation passed. Columns: " + df.columns.mkString(", "))
      true
    } catch {
      case e: Exception =>
        LogManager.getRootLogger.error("Schema validation error: " + e.getMessage)
        false
    }
  }

  def load(): Unit = {
    println("Loading data for flat file generation")
    try {
      LogManager.getRootLogger.info("Loading data from source")
      
      val sourceData = spark.read
        .option("inferSchema", "true")
        .csv(rootPath + "landing/gst_data/")
      
      if (validateSchema(sourceData)) {
        perioddf = sourceData
        LogManager.getRootLogger.info("Data loaded successfully: " + perioddf.count() + " records")
      } else {
        throw new Exception("Schema validation failed for source data")
      }
    } catch {
      case e: Exception =>
        LogManager.getRootLogger.error("Failed to load data: " + e.getMessage)
        errorCount = errorCount + 1
        throw e
    }
  }

  def doTransformations(): Unit = {
    println("Starting transformations for flat file generation")
    try {
      if (perioddf == null) {
        throw new Exception("Period data is null. Load data first.")
      }

      LogManager.getRootLogger.info("Applying transformations")
      
      val transformedDf = perioddf
        .withColumn("load_timestamp", lit(current_timestamp_dt))
        .withColumn("process_date", to_date(col("transaction_date"), "yyyy-MM-dd"))
        .withColumn("month", month(col("process_date")))
        .withColumn("year", year(col("process_date")))
        .withColumn("gstin_hash", md5(col("gstin")))
        .filter(col("status") === "VALID" || col("status") === "ACTIVE")
        .distinct()

      recordCount = transformedDf.count()
      LogManager.getRootLogger.info("Transformations completed. Processed records: " + recordCount)

      if (recordCount == 0) {
        throw new Exception("No records available after transformation")
      }

      finaldf = transformedDf
      LogManager.getRootLogger.info("Final DataFrame prepared for export")

    } catch {
      case e: Exception =>
        LogManager.getRootLogger.error("Error during transformations: " + e.getMessage)
        errorCount = errorCount + 1
        throw e
    }
  }

  def generateFlatFiles(): Unit = {
    println("Generating flat files")
    try {
      if (finaldf == null) {
        throw new Exception("Final DataFrame is null")
      }

      val outputPath = rootPath + "flatfiles/" + filter_section + "/" + current_timestamp_dt + "/"
      
      LogManager.getRootLogger.info("Generating flat files to: " + outputPath)
      
      val gstTypeDf = finaldf.select($"gst_type").distinct()
      gstTypeDf.collect().foreach { row =>
        val gstType = row.getAs[String]("gst_type")
        val filteredDf = finaldf.filter($"gst_type" === gstType)
        
        val fileName = filter_section + "_" + gstType + "_" + current_timestamp_dt + ".csv"
        val filePath = outputPath + fileName
        
        LogManager.getRootLogger.info("Writing flat file: " + filePath)
        
        filteredDf.coalesce(1)
          .write
          .mode(SaveMode.Overwrite)
          .option("header", "true")
          .option("delimiter", ",")
          .csv(filePath)
        
        fileCount = fileCount + 1
        LogManager.getRootLogger.info("Flat file generated: " + fileName)
      }

      LogManager.getRootLogger.info("All flat files generated successfully. Total files: " + fileCount)

    } catch {
      case e: Exception =>
        LogManager.getRootLogger.error("Error generating flat files: " + e.getMessage)
        errorCount = errorCount + 1
        throw e
    }
  }

  def generateSummaryReport(): Unit = {
    println("Generating summary report")
    try {
      val summaryPath = rootPath + "reports/flatfile_summary/"
      
      val summaryData = Seq(
        (filter_section, current_timestamp_dt, recordCount, fileCount, errorCount, "COMPLETED")
      ).toDF("section", "process_date", "record_count", "file_count", "error_count", "status")

      summaryData.coalesce(1)
        .write
        .mode(SaveMode.Append)
        .option("header", "true")
        .csv(summaryPath)

      LogManager.getRootLogger.info("Summary report generated at: " + summaryPath)
      LogManager.getRootLogger.info("=" * 50)
      LogManager.getRootLogger.info("FLAT FILE GENERATION SUMMARY")
      LogManager.getRootLogger.info("=" * 50)
      LogManager.getRootLogger.info("Section: " + filter_section)
      LogManager.getRootLogger.info("Process Date: " + current_timestamp_dt)
      LogManager.getRootLogger.info("Total Records: " + recordCount)
      LogManager.getRootLogger.info("Files Generated: " + fileCount)
      LogManager.getRootLogger.info("Errors: " + errorCount)
      LogManager.getRootLogger.info("=" * 50)

    } catch {
      case e: Exception =>
        LogManager.getRootLogger.error("Error generating summary report: " + e.getMessage)
    }
  }

  def save(): Unit = {
    println("Saving generated flatfiles")
    try {
      startDate = DateUtils.getDate()
      LogManager.getRootLogger.info("Save process started at: " + startDate)
      
      if (finaldf == null) {
        throw new Exception("No data to save. Run transformations first.")
      }

      generateFlatFiles()
      generateSummaryReport()
      
      endDate = DateUtils.getDate()
      LogManager.getRootLogger.info("Save process completed at: " + endDate)
      LogManager.getRootLogger.info("GST flat files saved successfully")

    } catch {
      case e: Exception =>
        LogManager.getRootLogger.error("Failed to save flatfiles: " + e.getMessage)
        errorCount = errorCount + 1
        throw e
    }
  }

  def execute(): Unit = {
    try {
      LogManager.getRootLogger.info("GST Flat File Generation Job Started")
      readConfig()
      load()
      doTransformations()
      save()
      LogManager.getRootLogger.info("GST Flat File Generation Job Completed Successfully")
    } catch {
      case e: Exception =>
        LogManager.getRootLogger.error("GST Flat File Generation Job Failed: " + e.getMessage)
        throw e
    }
  }
}
