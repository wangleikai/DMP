package com.dmp.report

import com.dmp.tools.ConfigHandler
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计日志文件中各省市的数据分布情况
  *sparksql 实现
  * @author WangLeiKai
  *         2018/10/24  14:57
  */
object LogDataAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("统计日志文件中各省市的数据分布情况")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    //读取数据
    val rawDataFrame = sqlContext.read.parquet(ConfigHandler.parquetPath)

    //按照需求进行相应的分析（注册临时表）
    rawDataFrame.registerTempTable("log")

    //根据省份和城市进行相应的分析
    val result = sqlContext.sql(
      """
        |select count(*) ct,provincename,cityname
        |from log group by provincename,cityname
      """.stripMargin)

    //FileHandler.deleteWillOutDir(sparkContext, ConfigHandler.logdataAnalysisResultJsonPath)

    //将结果写成json数据格式文件（将结果合并到一个分区）
    result.coalesce(1).write.json(ConfigHandler.logdataAnalysisResultJsonPath)

    //将结果写入到Mysql
    result.write.mode(SaveMode.Overwrite).jdbc(ConfigHandler.url, ConfigHandler.logdataAnalysisTableName, ConfigHandler.dbProps)

    sparkContext.stop()

  }
}
