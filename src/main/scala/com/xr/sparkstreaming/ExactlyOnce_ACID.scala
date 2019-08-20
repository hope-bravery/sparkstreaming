package com.xr.sparkstreaming

import java.time.LocalDateTime

import com.xr.sparkstreaming.utils.{Config, ParseLog}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.{ConnectionPool, DB}
import scalikejdbc._

/**
  * User:xr
  * Date:2019/8/20 2:54
  * Description:事务写入
  * 输入:Kafka
  * 处理:SparkStreaming
  * 输出:MySQL
  * 实现Exactly-Once
  * 1.偏移量自己管理,即enable.auto.commit=false,这里保存在mysql中;
  * 2.使用createDirectStream;
  * 3.事务输出:结果存储与Offset提交在Driver端同一MySQL事务中.
  **/

object ExactlyOnce_ACID extends Config {
    def main(args: Array[String]): Unit = {
        val brokers = config.getString("exactly-once.acid.brokers")
        val topic = config.getString("exactly-once.acid.topic")
        val groupId = config.getString("exactly-once.acid.group.id")

        //Kafka配置
        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> brokers,
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> groupId,
            "enable.auto.commit" -> (false: java.lang.Boolean),
            "auto.offset.reset" -> "none"
        )

        //在Driver端创建数据库连接池
        ConnectionPool.singleton("jdbc:mysql://spark03:3306/spark", "root", "root")

        val conf = new SparkConf().setAppName("Exactly-Once-ACID").setMaster("local")
        val ssc = new StreamingContext(conf, Seconds(5))

        val fromOffsets: Map[TopicPartition, Long] = DB.readOnly { implicit session =>
            sql"""select `partition`, offset from kafka_offset where topic=${topic}"""
                    .map {
                        rs => new TopicPartition(topic, rs.int("partition")) -> rs.long("offset")
                    }.list.apply().toMap
        }

        val messages: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Assign[String, String](fromOffsets.keys, kafkaParams, fromOffsets)
        )

        messages.foreachRDD { rdd =>
            val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            val result: Array[(LocalDateTime, Int)] = ParseLog.processLogs(rdd).collect()

            DB.localTx { implicit session =>
                result.foreach { case (time, count) =>
                    sql"""
                         insert into error_log(log_time, log_count)
                         value (${time}, ${count})
                         on duplicate key update log_count = log_count + values(log_count)
                    """.update.apply()
                }

                offsetRanges.foreach { offsetRange =>

                    /**
                      * sql"""
                      * insert ignore into kafka_offset(topic, `partition`, offset)
                      * value(${topic}, ${offsetRange.partition}, ${offsetRange.fromOffset})
                      * """.update().apply()
                      */

                    val affectedRows =
                        sql"""
                            update kafka_offset set offset = ${offsetRange.untilOffset}
                            where topic = ${topic} and `partition` = ${offsetRange.partition} and offset = ${offsetRange.fromOffset}
                        """.update.apply()

                    if (affectedRows != 1)
                        throw new Exception(s"Commit kafka topic :${topic} failed!")
                }
            }
        }
        ssc.start()
        ssc.awaitTermination()
    }
}
