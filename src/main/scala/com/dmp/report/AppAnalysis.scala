package com.dmp.report

import com.dmp.tools.{ConfigHandler, MysqlHandler, RptKpiTools}
import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用广播变量 查看App情况
  * @author WangLeiKai
  *         2018/10/25  9:44
  */

case class AppAreaAnalysis(
                           appname: String,
                              rawReq: Double,
                              effReq: Double,
                              adReq: Double,
                              rtbReq: Double,
                              winReq: Double,
                              adShow: Double,
                              adClick: Double,
                              adCost: Double,
                              adPayment: Double
                             )
object AppAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[*]")
    // RDD 序列化到磁盘 worker与worker之间的数据传输
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sparkContext = new SparkContext(sparkConf)

    // 字典文件
    val dictMap = sparkContext.textFile("D:\\data\\资料 --- 广告项目\\资料 --- 广告项目\\app_dict.txt")
    val value = dictMap.map(_.split("\t")).filter(_.length >= 5)
    val tuples: Map[String, String] = value.map(line => {
      val appid = line(4)
      val appname = line(1)
      //名称  id
      (appid, appname)
    }).collect().toMap

    val app_dict = sparkContext.broadcast(tuples)

    val sqlContext = new SQLContext(sparkContext)

    val rawDataFrame = sqlContext.read.parquet(ConfigHandler.parquetPath)

    import sqlContext.implicits._
    val resultDF: DataFrame = rawDataFrame.filter("appid != null or appname != null or appname != '' ").map(row => {
      val aid = row.getAs[String]("appid")
      var aname = row.getAs[String]("appname")

      if (StringUtils.isEmpty(aname)){
        aname = app_dict.value.getOrElse(aid,aid)
      }


      (aname, RptKpiTools.offLineKpi(row))
    }).reduceByKey((list1, list2) => list1 zip list2 map (tp => tp._1 + tp._2))
        .map(rs => AppAreaAnalysis(rs._1, rs._2(0), rs._2(1), rs._2(2), rs._2(3), rs._2(4), rs._2(5), rs._2(6), rs._2(7), rs._2(8)))
      .toDF


    MysqlHandler.save2db(resultDF,ConfigHandler.mediaAnalysisTableName)

    sparkContext.stop()
  }
}
