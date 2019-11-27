package com.wang.gmall.util.app

import redis.clients.jedis.Jedis

object test {
  def main(args: Array[String]): Unit = {
    val jedis = new Jedis("hadoop102")
    val redis: String = jedis.set("1","1")
    println(redis.getBytes)
    jedis.close()
  }
}
