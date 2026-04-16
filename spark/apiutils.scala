package com.example.spark

object apiutils {
  val logManager = LogManager.getRootLogger
  val spark: SparkSession = SparkConfig.sparkSession
  val record_date: String = DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now())
  val batch_no: String = DateTimeFormat.forPattern("yyyyMMddHHmmss").print(DateTime.now())

  def apipush(url: String, headers: Map[String, String], params: Map[String, String]): HttpResponse[String] = {
    logManager.info("------------Pushing data---------------")
    logManager.info(s"Calling API: $url")
    logManager.info(s"Headers: $headers")
    logManager.info(s"Parameters: $params")

    val response = Http(url).headers(headers).params(params).timeout(100000, 100000).asString
    response
  }

  def responsestore(config_app_name: String, config_statusPath: String, config_errorTablePath: String, response: HttpResponse[String], record_count: Long, operation: String, apiv: String): Unit = {
    val postCounter = 1
    val request_no = DateTimeFormat.forPattern("yyyyMMddHHmmssSSS").print(DateTime.now())
    if (response.code == 200) {
      logManager.info("response:" + response.body)
      val statusEntry = Seq(config_app_name).toDF("app_name").withColumn("request_key", concat(lit(request_no), lit(postCounter), lit(1))).withColumn("record_count", lit(record_count)).withColumn("request_no", lit(request_no)).withColumn("request_counter", lit(postCounter)).withColumn("success_count", lit(record_count).cast(LongType)).withColumn("status", lit("env:" + apiv + " & op:" + operation))
      statusEntry.write.mode(SaveMode.Append).orc(config_statusPath + "status_record_date_dt=" + record_date + "/batch_no=" + batch_no)
    } else {
      logManager.info("Error occurred: " + response.body)
      val df = spark.read.json(Seq(response.body).toDS)
      df.write.mode(SaveMode.Append).orc(config_errorTablePath + "part_record_date_dt=" + record_date + "/batch_no=" + batch_no + "/")

      throw new Exception("API Error. Unknown error: " + response.body)
    }
    logManager.info("Response Code: " + response.code)
  }

}
