package com.dmp.utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  * @author WangLeiKai
  *         2018/10/25  14:09
  */
object JedisPools {

  private val jedisPool = new JedisPool(new GenericObjectPoolConfig(), "192.168.59.11", 6379)


  def getJedis() = {
    val jedis = jedisPool.getResource
    jedis.select(8)
    jedis
  }


}
