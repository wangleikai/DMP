package com.dmp.report

import com.dmp.tools.ConfigHandler
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * 统计日志文件中各省市的数据分布情况
  * spark core 实现
  * @author WangLeiKai
  *         2018/10/24  15:30
  */

case class ReportLogDataAnalysis(provinceName:String, cityName:String, ct:Int)
object LogDataAnalysisCore {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("统计日志文件中各省市的数据分布情况")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    //读取数据
    val rawDataFrame: DataFrame = sqlContext.read.parquet(ConfigHandler.parquetPath)


    val resultRDD= rawDataFrame.map(line => {
      val pname = line.getAs[String]("provincename")
      val cname = line.getAs[String]("cityname")

      ((pname, cname), 1)
    }).reduceByKey(_ + _)

    import sqlContext.implicits._
    val dF = resultRDD.map(tp => {
      ReportLogDataAnalysis(tp._1._1, tp._1._2, tp._2)
    }).toDF()
    // 判断结果存储路径是否存在，如果存在则删除
    val hadoopConfiguration = sparkContext.hadoopConfiguration
    val fs = FileSystem.get(hadoopConfiguration)

    val resultPath = new Path(ConfigHandler.parquetPath)
    if(fs.exists(resultPath)) {
      fs.delete(resultPath, true)
    }



    dF.coalesce(1).write.json(ConfigHandler.logdataAnalysisResultJsonPath)

    dF.show()
    sparkContext.stop()
  }
}
