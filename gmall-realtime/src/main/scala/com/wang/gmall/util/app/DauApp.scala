package com.wang.gmall.util.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.wang.gmall.common.constant.GmallConstant
import com.wang.gmall.util.{MyKafkaUtil, RedisUtil, StartUpLog}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object DauApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP, ssc)

    val startUpLogDStream: DStream[StartUpLog] = inputDStream.map(record => {
      val jsonString: String = record.value()
      val startUpLog = JSON.parseObject(jsonString, classOf[StartUpLog])
      val format = new SimpleDateFormat("yyyy-MM-dd HH")
      val date: Date = new Date(startUpLog.ts)
      val dates: String = format.format(date)
      val dayAndmonth = dates.split(" ")
      startUpLog.logDate = dayAndmonth(0)
      startUpLog.logHour = dayAndmonth(1)
      startUpLog
    })

    startUpLogDStream.cache()


    //批次间去重
    val filterDStream: DStream[StartUpLog] = startUpLogDStream.transform {
      rdd =>
        val jedis: Jedis = RedisUtil.getJedisClient
        val str = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
        val key = "dau:" + str
        val dauSet: util.Set[String] = jedis.smembers(key)
        jedis.close()
        val dauBC = ssc.sparkContext.broadcast(dauSet)
        println(rdd.count())
        val filterRDD: RDD[StartUpLog] = rdd.filter({
          startuplog => {
            val dauMidSet: util.Set[String] = dauBC.value
            !dauMidSet.contains(startuplog.mid)
          }
        })
        println(filterRDD.count())
        filterRDD
    }

    val groupDStream: DStream[(String, Iterable[StartUpLog])] = filterDStream.map(startuplog=>(startuplog.mid,startuplog)).groupByKey()

    //组内排序，并取组内第一个人
    val groupMidDStream: DStream[StartUpLog] = groupDStream.flatMap {
      case (mid, startupLog) => {
        startupLog.toList.sortWith(
          (left, right) => {
            left.ts < right.ts
          }
        ).take(1)
      }
    }

    groupMidDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(startlog=>{
        val jedis: Jedis = RedisUtil.getJedisClient
        for (elem <- startlog) {
          val key = "dau:" + elem.logDate
          jedis.sadd(key,elem.mid)
        }
        jedis.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
