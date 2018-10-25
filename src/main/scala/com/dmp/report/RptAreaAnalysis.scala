package com.dmp.report

import java.util.Properties

import com.dmp.tools.{ConfigHandler, MysqlHandler, RptKpiTools}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 根据地域分布统计kpi指标信息
  * sparkcore实现
  * @author WangLeiKai
  *         2018/10/24  17:01
  */
case class ReportAreaAnalysis(provinceName: String,
                              cityName: String,
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
object RptAreaAnalysis {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("根据地域分布统计kpi指标信息")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    //读取数据
    val rawDataFrame = sqlContext.read.parquet(ConfigHandler.parquetPath)

    import sqlContext.implicits._
    val resultDF: DataFrame = rawDataFrame.map(row => {
      val pname = row.getAs[String]("provincename")
      val cname = row.getAs[String]("cityname")

      ((pname, cname), RptKpiTools.offLineKpi(row))
    }).reduceByKey((list1, list2) => list1 zip list2 map (tp => tp._1 + tp._2))
      .map(rs => ReportAreaAnalysis(rs._1._1, rs._1._2, rs._2(0), rs._2(1), rs._2(2), rs._2(3), rs._2(4), rs._2(5), rs._2(6), rs._2(7), rs._2(8)))
      .toDF

    val load = ConfigFactory.load()
    val props = new Properties()
    props.setProperty("user", load.getString("db.user"))
    props.setProperty("password", load.getString("db.password"))

    // 将结果写入到mysql的表中
    //dF.write.mode(SaveMode.Overwrite).jdbc(load.getString("db.url"), load.getString("db.tableName"), props)
    MysqlHandler.save2db(resultDF, ConfigHandler.areaAnalysisTableName, 1)

    sparkContext.stop()



  }



}
