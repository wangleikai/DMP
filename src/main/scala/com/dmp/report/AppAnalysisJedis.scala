package com.dmp.report

import com.dmp.tools.{ConfigHandler, MysqlHandler, RptKpiTools}
import com.dmp.utils.JedisPools
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 始用redis  查看APP的使用情况
  *
  * 数据中间有appname和appid为空的情况
  * @author WangLeiKai
  *         2018/10/25  9:44
  */


object AppAnalysisJedis {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[*]")
    // RDD 序列化到磁盘 worker与worker之间的数据传输
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sparkContext = new SparkContext(sparkConf)
    val sQLContext = new SQLContext(sparkContext)



    val rawDataFrame = sQLContext.read.parquet(ConfigHandler.parquetPath).filter("appid != null or appname != null or appname != ''")

    rawDataFrame.registerTempTable("log")

    sQLContext.udf.register("NotEmptyAppName",RptKpiTools.notEmptyAppName)

    val result = sQLContext.sql(
      """
          select NotEmptyAppName(appid,appname) appname,
          sum(case when requestmode = 1 and processnode >= 1 then 1 else 0 end) rawReq,
          sum(case when requestmode = 1 and processnode >= 2 then 1 else 0 end) effReq,
          sum(case when requestmode = 1 and processnode = 3 then 1 else 0 end) adReq,
          sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 and adorderid != 0 then 1 else 0 end) rtbReq,
          sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then 1 else 0 end) winReq,
          sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) adShow,
          sum(case when requestmode = 3 and iseffective = 1 then 1 else 0 end) adClick,
          sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice/1000  else 0 end) adCost,
          sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment/1000  else 0 end) adPayment
          from log
          group by NotEmptyAppName(appid,appname)
      """.stripMargin)

    MysqlHandler.save2db(result,ConfigHandler.mediaAnalysisTableName)

    sparkContext.stop()

  }
}
