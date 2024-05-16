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

package org.apache.spark.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.Level;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;

public class StructuredLoggerSuite extends LoggerSuiteBase {

  private static final SparkLogger LOGGER =
    SparkLoggerFactory.getLogger(StructuredLoggerSuite.class);

  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
  private String compactAndToRegexPattern(Level level, String json) {
    try {
      return JSON_MAPPER.readTree(json).toString()
         .replace("<level>", level.toString())
         .replace("<className>", className())
         .replace("<timestamp>", "[^\"]+")
         .replace("\"<stacktrace>\"", ".*")
         .replace("{", "\\{") + "\n";
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  SparkLogger logger() {
    return LOGGER;
  }

  @Override
  String className() {
    return StructuredLoggerSuite.class.getSimpleName();
  }

  @Override
  String logFilePath() {
    return "target/structured.log";
  }

  @Override
  String expectedPatternForBasicMsg(Level level) {
    return compactAndToRegexPattern(level, """
      {
        "ts": "<timestamp>",
        "level": "<level>",
        "msg": "This is a log message",
        "logger": "<className>"
      }""");
  }

  @Override
  String expectedPatternForBasicMsgWithException(Level level) {
    return compactAndToRegexPattern(level, """
      {
        "ts": "<timestamp>",
        "level": "<level>",
        "msg": "This is a log message",
        "exception": {
          "class": "java.lang.RuntimeException",
          "msg": "OOM",
          "stacktrace": "<stacktrace>"
        },
        "logger": "<className>"
      }""");
  }

  @Override
  String expectedPatternForMsgWithMDC(Level level) {
    return compactAndToRegexPattern(level, """
      {
        "ts": "<timestamp>",
        "level": "<level>",
        "msg": "Lost executor 1.",
        "context": {
          "executor_id": "1"
        },
        "logger": "<className>"
      }""");
  }

  @Override
  String expectedPatternForMsgWithMDCs(Level level) {
    return compactAndToRegexPattern(level, """
      {
        "ts": "<timestamp>",
        "level": "<level>",
        "msg": "Lost executor 1, reason: the shuffle data is too large",
        "context": {
          "executor_id": "1",
          "reason": "the shuffle data is too large"
        },
        "logger": "<className>"
      }""");
  }

  @Override
  String expectedPatternForMsgWithMDCsAndException(Level level) {
    return compactAndToRegexPattern(level, """
      {
        "ts": "<timestamp>",
        "level": "<level>",
        "msg": "Lost executor 1, reason: the shuffle data is too large",
        "context": {
          "executor_id": "1",
          "reason": "the shuffle data is too large"
        },
        "exception": {
          "class": "java.lang.RuntimeException",
          "msg": "OOM",
          "stacktrace": "<stacktrace>"
        },
        "logger": "<className>"
      }""");
  }

  @Override
  String expectedPatternForMsgWithMDCValueIsNull(Level level) {
    return compactAndToRegexPattern(level, """
      {
        "ts": "<timestamp>",
        "level": "<level>",
        "msg": "Lost executor null.",
        "context": {
          "executor_id": null
        },
        "logger": "<className>"
      }""");
  }

  @Override
  String expectedPatternForExternalSystemCustomLogKey(Level level) {
    return compactAndToRegexPattern(level, """
      {
        "ts": "<timestamp>",
        "level": "<level>",
        "msg": "External system custom log message.",
        "context": {
          "custom_log_key": "External system custom log message."
        },
        "logger": "<className>"
      }""");
  }
}

