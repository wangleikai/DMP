package com.dmp.report

import com.dmp.tools.ConfigHandler
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 根据地域分布统计kpi指标信息
  * sparksql实现
  * 自定义udf
  * @author WangLeiKai
  *         2018/10/25  8:29
  */
object RptAreaAnalysisSqlV2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("根据地域分布统计kpi指标信息")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    //读取数据
    val rawDataFrame = sqlContext.read.parquet(ConfigHandler.parquetPath)

    sqlContext.udf.register("area_function",(boolean : Boolean,result : Int) => if(boolean) result else 0)
    rawDataFrame.registerTempTable("t_rptAreaAnalysisSql")

    val result = sqlContext.sql(
      """
          select provincename,cityname,
          sum(area_function(requestmode = 1 and processnode >= 1,1)) rawReq,
          sum(case when requestmode = 1 and processnode >= 2 then 1 else 0 end) effReq,
          sum(case when requestmode = 1 and processnode = 3 then 1 else 0 end) adReq,
          sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 and adorderid != 0 then 1 else 0 end) rtbReq,
          sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then 1 else 0 end) winReq,
          sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) adShow,
          sum(case when requestmode = 3 and iseffective = 1 then 1 else 0 end) adClick,
          sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice/1000  else 0 end) adClick,
          sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment/1000  else 0 end) adPayment
          from t_rptAreaAnalysisSql
          group by provincename,cityname
      """.stripMargin)

    result.show()

    sparkContext.stop()


  }
}
