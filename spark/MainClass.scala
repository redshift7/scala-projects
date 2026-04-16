package com.example.spark

object MainClass {
  /**
   * This method selects which process object to run based on input parameters.
   *
   * @param args 1. Object that has to be executed
   *             2. Parameters for the main method of the respective main class
   *
   *             Version 1.0 //Initial code
   *
   */
  def main(args: Array[String]): Unit = {
    val objPicker = args(0)
    val objAttr = args.drop(1)
    val logManager = LogManager.getRootLogger

    objPicker match {
      case "LandingToRaw_Delta" => val obj = LandingToRaw_Delta; obj.execute(objAttr)
      case _ => SparkConfig.getSparkSession(); logManager.error("CTR101: No valid process selected")

    }
  }
}
