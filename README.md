# Scala Projects - Apache Spark Data Processing

Data processing and ETL pipelines using Apache Spark and Scala.

## Overview

This repository contains Scala-based Spark jobs for:
- Tax data processing and flatfile generation
- Landing zone to raw layer transformations with SCD Type 2 handling
- External system data integration and COD (Cash on Delivery) processing
- Utility functions for common data processing operations

## Project Structure

\\\
scala-projects/
├── spark/                                    # Main Spark application
│   ├── src/main/scala/
│   │   ├── apiutils.scala                   # Utility functions
│   │   ├── MainClass.scala                  # Entry point
│   │   ├── LandingToRawSCDJmtoms.scala      # Landing to raw with SCD
│   │   ├── TrustedToExternalCOD.scala       # External COD processing
│   │   └── gst/
│   │       ├── GenerateFlat.scala           # Flatfile generation
│   │       └── getJsonFromGst.scala         # JSON processing
│   └── build.gradle                        # Gradle build configuration
├── .gitignore
└── README.md
\\\

## Prerequisites

- Scala 2.11.12+
- Apache Spark 2.2.0+
- Java 8 or higher
- Gradle 4.0+
- Hadoop 2.7+

## Installation

\\\ash
git clone https://github.com/redshift7/scala-projects.git
cd scala-projects/spark
\\\

## Configuration

Set environment variables before running:

\\\ash
export ASP_CLIENT_ID="[YOUR_CLIENT_ID]"
export ASP_CLIENT_SECRET_KEY="[YOUR_SECRET_KEY]"
export GST_MAIL_FROM="[YOUR_EMAIL]"
export GST_MAIL_TO="[YOUR_EMAIL_LIST]"
export GST_API_BASE_URL="http://aspapi.example.com:8080"
\\\

## License

[Add your license]
