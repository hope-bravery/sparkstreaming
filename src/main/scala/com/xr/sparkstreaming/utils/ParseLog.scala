package com.xr.sparkstreaming.utils

import java.time.LocalDateTime
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.time.temporal.ChronoUnit

import com.xr.sparkstreaming.entity.Log
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD

/**
  * User:xr
  * Date:2019/8/20 4:37
  * Description:日志解析
  **/
object ParseLog extends Serializable {
    def processLogs(message: RDD[ConsumerRecord[String, String]]): RDD[(LocalDateTime, Int)] = {
        message.map(_.value)
                .flatMap(parseLog)
                .filter(_.level == "ERROR")
                .map(log => log.time.truncatedTo(ChronoUnit.MINUTES) -> 1)
                .reduceByKey(_ + _)
    }

    val logPattern = "^(.{19}) ([A-Z]+).*".r
    val dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    def parseLog(line: String): Option[Log] = {
        line match {
            case logPattern(timeString, level) => {
                val timeOption = try {
                    Some(LocalDateTime.parse(timeString, dateTimeFormatter))
                } catch {
                    case _: DateTimeParseException => None
                }
                timeOption.map(Log(_, level))
            }
            case _ => {
                None
            }
        }
    }
}
