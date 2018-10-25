package com.dmp.tools

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.avro.data.Json
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}




/**
  * 统计数据中的省份和城市
  * @author WangLeiKai
  *         2018/10/24  10:44
  */
object ProvinceAndCity {

  val resultOutputPath = "d://data//out"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("ProvinceAndCity")

    val sc = new SparkContext(conf)

    val sQLContext = new SQLContext(sc)

    sQLContext.setConf("spark.sql.parquet.compression.codec", "snappy")

    val df: DataFrame = sQLContext.read.json("d://out//part-r-00000-35393e82-9539-4411-9baa-229c5f4cae4f")

    df.registerTempTable("t_json")

    val result: DataFrame = sQLContext.sql("select provincename,cityname,count(*) as ct from t_json group by provincename,cityname")


    // 判断结果存储路径是否存在，如果存在则删除
    val hadoopConfiguration = sc.hadoopConfiguration
    val fs = FileSystem.get(hadoopConfiguration)

    val resultPath = new Path(resultOutputPath)
    if(fs.exists(resultPath)) {
      fs.delete(resultPath, true)
    }


    // 将结果存储的成json文件
    //result.coalesce(1).write.json(resultOutputPath)

    val load = ConfigFactory.load()
    val props = new Properties()
    props.setProperty("user", load.getString("jdbc.user"))
    props.setProperty("password", load.getString("jdbc.password"))

    // 将结果写入到mysql的 rpt_pc_count 表中
    result.write/*.mode(SaveMode.Append)*/.jdbc(load.getString("jdbc.url"), load.getString("jdbc.tableName"), props)

    sc.stop()
  }
}
