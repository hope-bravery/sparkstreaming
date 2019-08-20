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
  * Date:2019/8/20 2:21
  * Description:幂等输出
  * 输入:Kafka
  * 处理:SparkStreaming
  * 输出:MySQL
  * 实现Exactly-Once
  * 1.偏移量自己管理,即enable.auto.commit=false,这里保存在mysql中(代码中进行了打印);
  * 2.使用createDirectStream;
  * 3.幂等输出:同样的数据,无论输出多少次,效果一样.一般要结合外部存储主键、唯一键实现.
  **/

object ExactlyOnce_Idempotent extends Config {
    def main(args: Array[String]): Unit = {
        val brokers = config.getString("exactly-once.idempotent.brokers")
        val topic = config.getString("exactly-once.idempotent.topic")
        val groupId = config.getString("exactly-once.idempotent.group.id")

        //Kafka配置
        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> brokers,
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> groupId,
            "enable.auto.commit" -> (true: java.lang.Boolean),
            "auto.offset.reset" -> "none"
        )

        //在Driver端创建数据库连接池
        ConnectionPool.singleton("jdbc:mysql://spark03:3306/spark", "root", "root")

        val conf = new SparkConf().setAppName("Exactly-Once-Idempotent").setMaster("local")
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
            val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            val result: Array[(LocalDateTime, Int)] = ParseLog.processLogs(rdd).collect()

            DB.autoCommit { implicit session =>
                result.foreach { case (time, count) =>
                    sql"""
                         insert into error_log(log_time, log_count)
                         value (${time}, ${count})
                         on duplicate key update log_count = log_count + values(log_count)
                    """.update.apply()
                }
            }

            offsetRanges.foreach(offsetRange => {
                println(s"topic:${offsetRange.topic}\tpartition:${offsetRange.partition}\tfromOffset:${offsetRange.fromOffset}\tuntilOffset:${offsetRange.untilOffset}")
            })
        }

        ssc.start()
        ssc.awaitTermination()
    }
}