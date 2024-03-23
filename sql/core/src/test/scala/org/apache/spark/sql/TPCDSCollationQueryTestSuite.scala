/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import java.io.File
import java.util.Locale

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.catalyst.util.{fileToString, resourceToString, stringToFile}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.TestSparkSession
import org.apache.spark.tags.ExtendedSQLTest

@ExtendedSQLTest
class TPCDSCollationQueryTestSuite extends QueryTest with TPCDSBase with SQLQueryTestHelper {

  private val tpcdsDataPath = sys.env.get("SPARK_TPCDS_DATA")

  // To make output results deterministic
  override protected def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.SHUFFLE_PARTITIONS.key, "1")

  protected override def createSparkSession: TestSparkSession = {
    new TestSparkSession(new SparkContext("local[1]", this.getClass.getSimpleName, sparkConf))
  }

  protected val baseResourcePath = {
    getWorkspaceFilePath(
      "sql",
      "core",
      "src",
      "test",
      "resources",
      "tpcds-query-collated-results").toFile.getAbsolutePath
  }

  val defaultDb = "tpcds_dev"
  val collatedDb = "tpcds_collated_dev"
  val normalizedDb = "tpcds_normalized_dev"
  val randomizeCase = "RANDOMIZE_CASE"

  override def createTables(): Unit = {
    spark.sql(s"DROP DATABASE IF EXISTS $defaultDb CASCADE")
    spark.sql(s"DROP DATABASE IF EXISTS $collatedDb CASCADE")
    spark.sql(s"DROP DATABASE IF EXISTS $normalizedDb CASCADE")

    spark.sql(s"CREATE DATABASE $defaultDb")
    spark.sql(s"CREATE DATABASE $collatedDb")
    spark.sql(s"CREATE DATABASE $normalizedDb")

    spark.udf.register(
      randomizeCase,
      functions.udf((s: String) => {
        val random = new scala.util.Random()
        s.map(c => if (random.nextBoolean()) c.toUpper else c.toLower)
      }))

    super.createTables()
  }

  override def createTable(
      spark: SparkSession,
      tableName: String,
      format: String = "parquet",
      options: Seq[String]): Unit = {

    val columns = tableColumns(tableName)
      .split("\n")
      .filter(_.trim.nonEmpty)
      .map { column =>
        if (column.trim.split("\\s+").length != 2) {
          throw new IllegalArgumentException(s"Invalid column definition: $column")
        }
        val Array(name, colType) = column.trim.split("\\s+")
        (name, colType.replaceAll(",$", ""))
      }

    def initTable(db: String, collation: String, transformationFn: String): Unit = {
      spark.sql(
        s"""
           |CREATE TABLE $db.$tableName
           |(${collateStringColumns(columns, collation)})
           |USING $format
           |${options.mkString("\n")}
         """.stripMargin)

      val transformedColumns = columns.map { case (name, colType) =>
        if (isTextColumn(colType)) {
          s"$transformationFn($name) AS $name"
        } else {
          name
        }
      }.mkString(", ")

      spark.sql(
        s"""
          |INSERT INTO TABLE $db.$tableName
          |SELECT $transformedColumns
          |FROM $defaultDb.$tableName
          |""".stripMargin)
    }

    spark.sql(
      s"""
         |CREATE TABLE $defaultDb.$tableName (${tableColumns(tableName)})
         |USING $format
         |${options.mkString("\n")}
   """.stripMargin)

    initTable(collatedDb, "UTF8_BINARY_LCASE", randomizeCase)
    initTable(normalizedDb, "UTF8_BINARY", "UPPER")
    val x = 3
  }

  override def dropTables(): Unit = {
    spark.sql(s"DROP DATABASE IF EXISTS $collatedDb CASCADE")
    spark.sql(s"DROP DATABASE IF EXISTS $normalizedDb CASCADE")
  }

  private def collateStringColumns(
      columns: Array[(String, String)],
      collation: String): String = {
    columns
      .map { case (name, colType) =>
        if (isTextColumn(colType)) {
          s"$name STRING COLLATE $collation"
        } else {
          s"$name $colType"
        }
      }
      .mkString(",\n")
  }

  private def isTextColumn(columnType: String): Boolean = {
    columnType.toUpperCase(Locale.ROOT).contains("CHAR")
  }

  private def runQuery(query: String, goldenFile: File, conf: Map[String, String]): Unit = {
    withSQLConf(conf.toSeq: _*) {
      try {
        val collatedOutput = getQueryOutput(collatedDb, query)
        val normalizedOutput = getQueryOutput(normalizedDb, query)
        val queryString = query.trim

        if (regenerateGoldenFiles || true) {
//          writeNewGoldenFile(collatedOutput, normalizedOutput, goldenFile)
        }

        val (expectedCollatedOutput, expectedNormalizedOutput) = readGoldenFile(goldenFile)

        assertResult(expectedCollatedOutput, s"Collated output did not match\n$queryString") {
          collatedOutput
        }
        assertResult(expectedNormalizedOutput, s"Normalized output did not match\n$queryString") {
          normalizedOutput
        }

      } catch {
        case e: Throwable =>
          val configs = conf.map { case (k, v) =>
            s"$k=$v"
          }
          throw new Exception(s"${e.getMessage}\nError using configs:\n${configs.mkString("\n")}")
      }
    }
  }

  private def getQueryOutput(db: String, query: String): String = {
    spark.sql(s"USE $db")

    val (_, output) = handleExceptions(getNormalizedQueryExecutionResult(spark, query))
    output.mkString("\n").replaceAll("\\s+$", "")
  }

  private def writeNewGoldenFile(
      collatedOutput: String,
      normalizedOutput: String,
      goldenFile: File): Unit = {
    val goldenOutput = {
      s"-- Automatically generated by ${getClass.getSimpleName}\n\n" +
        s"-- !query on collated db\n" +
        collatedOutput + "\n" +
        s"-- !query on normalized db\n" +
        normalizedOutput +
        "\n"
    }
    val parent = goldenFile.getParentFile
    if (!parent.exists()) {
      assert(parent.mkdirs(), "Could not create directory: " + parent)
    }
    stringToFile(goldenFile, goldenOutput)
  }

  private def readGoldenFile(goldenFile: File): (String, String) = {
    val goldenOutput = fileToString(goldenFile)
    val segments = goldenOutput.split("-- !query.*\n")

    // query has 3 segments, plus the header
    assert(
      segments.size == 3,
      s"Expected 3 blocks in result file but got ${segments.size}. " +
        "Try regenerate the result files.")

    (segments(1).trim, segments(2).replaceAll("\\s+$", ""))
  }

//  if (tpcdsDataPath.nonEmpty) {
    tpcdsQueries.foreach { name =>
      val queryString = resourceToString(
        s"tpcds/$name.sql",
        classLoader = Thread.currentThread().getContextClassLoader)
      test(name) {
        val goldenFile = new File(s"$baseResourcePath/v1_4", s"$name.sql.out")
        runQuery(queryString, goldenFile, Map.empty)
      }
    }

    tpcdsQueriesV2_7_0.foreach { name =>
      val queryString = resourceToString(
        s"tpcds-v2.7.0/$name.sql",
        classLoader = Thread.currentThread().getContextClassLoader)
      test(s"$name-v2.7") {
        val goldenFile = new File(s"$baseResourcePath/v2_7", s"$name.sql.out")
        runQuery(queryString, goldenFile, Map.empty)
      }
    }
//  } else {
//    ignore("skipped because env 'SPARK_TPCDS_DATA' is not set") {}
//  }
}
