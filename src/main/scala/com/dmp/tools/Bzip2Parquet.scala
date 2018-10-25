package com.dmp.tools

import com.dmp.utils.{NBF, SchemaUtils}
import org.apache.hadoop.mapreduce.v2.jobhistory.FileNameIndexUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将原始日志文件转换成parquet文件格式
  * 采用snappy压缩格式
  */
object Bzip2Parquet {

    def main(args: Array[String]): Unit = {

        val logInputPath = "D:\\data\\资料 --- 广告项目\\资料 --- 广告项目\\2016-10-01_06_p1_invalid.1475274123982\\2016-10-01_06_p1_invalid.1475274123982.log"

        val resultOutputPath = "d://out"

        val compressionCode = "snappy"

        // 2 创建sparkconf->sparkContext
        val sparkConf = new SparkConf()
        sparkConf.setAppName(s"${this.getClass.getSimpleName}")
        sparkConf.setMaster("local[*]")
        // RDD 序列化到磁盘 worker与worker之间的数据传输
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        val sc = new SparkContext(sparkConf)

        val sQLContext = new SQLContext(sc)
        sQLContext.setConf("spark.sql.parquet.compression.codec", "snappy")


        // 3 读取日志数据
        val rawdata = sc.textFile(logInputPath)

        import com.dmp.utils.StrUtils._

        // 4 根据业务需求对数据进行ETL  xxxx,x,x,x,x,,,,,
        val dataRow: RDD[Row] = rawdata
          .map(line => line.split(",", line.length))
          .filter(_.length >= 85)
          .map(arr => {
              Row(
                  arr(0), //	sessionid: String,
                  arr(1).toIntPlus, //	advertisersid: Int,
                  arr(2).toIntPlus, //	adorderid: Int,
                  arr(3).toIntPlus, //	adcreativeid: Int,
                  arr(4).toIntPlus, //	adplatformproviderid: Int,
                  arr(5), //	sdkversion: String,
                  arr(6), //	adplatformkey: String,
                  arr(7).toIntPlus, //	putinmodeltype: Int,
                  arr(8).toIntPlus, //	requestmode: Int,
                  arr(9).toDoublePlus, //	adprice: Double,
                  arr(10).toDoublePlus, //		adppprice: Double,
                  arr(11), //		requestdate: String,
                  arr(12), //		ip: String,
                  arr(13), //		appid: String,
                  arr(14), //		appname: String,
                  arr(15), //		uuid: String,
                  arr(16), //		device: String,
                  arr(17).toIntPlus, //		client: Int,
                  arr(18), //		osversion: String,
                  arr(19), //		density: String,
                  arr(20).toIntPlus, //		pw: Int,
                  arr(21).toIntPlus, //		ph: Int,
                  arr(22), //		long: String,
                  arr(23), //		lat: String,
                  arr(24), //		provincename: String,
                  arr(25), //		cityname: String,
                  arr(26).toIntPlus, //		ispid: Int,
                  arr(27), //		ispname: String,
                  arr(28).toIntPlus, //		networkmannerid: Int,
                  arr(29), //		networkmannername: String,
                  arr(30).toIntPlus, //		iseffective: Int,
                  arr(31).toIntPlus, //		isbilling: Int,
                  arr(32).toIntPlus, //		adspacetype: Int,
                  arr(33), //		adspacetypename: String,
                  arr(34).toIntPlus, //		devicetype: Int,
                  arr(35).toIntPlus, //		processnode: Int,
                  arr(36).toIntPlus, //		apptype: Int,
                  arr(37), //		district: String,
                  arr(38).toIntPlus, //		paymode: Int,
                  arr(39).toIntPlus, //		isbid: Int,
                  arr(40).toDoublePlus, //		bidprice: Double,
                  arr(41).toDoublePlus, //		winprice: Double,
                  arr(42).toIntPlus, //		iswin: Int,
                  arr(43), //		cur: String,
                  arr(44).toDoublePlus, //		rate: Double,
                  arr(45).toDoublePlus, //		cnywinprice: Double,
                  arr(46), //		imei: String,
                  arr(47), //		mac: String,
                  arr(48), //		idfa: String,
                  arr(49), //		openudid: String,
                  arr(50), //		androidid: String,
                  arr(51), //		rtbprovince: String,
                  arr(52), //		rtbcity: String,
                  arr(53), //		rtbdistrict: String,
                  arr(54), //		rtbstreet: String,
                  arr(55), //		storeurl: String,
                  arr(56), //		realip: String,
                  arr(57).toIntPlus, //		isqualityapp: Int,
                  arr(58).toDoublePlus, //		bidfloor: Double,
                  arr(59).toIntPlus, //		aw: Int,
                  arr(60).toIntPlus, //		ah: Int,
                  arr(61), //		imeimd5: String,
                  arr(62), //		macmd5: String,
                  arr(63), //		idfamd5: String,
                  arr(64), //		openudidmd5: String,
                  arr(65), //		androididmd5: String,
                  arr(66), //		imeisha1: String,
                  arr(67), //		macsha1: String,
                  arr(68), //		idfasha1: String,
                  arr(69), //		openudidsha1: String,
                  arr(70), //		androididsha1: String,
                  arr(71), //		uuidunknow: String,
                  arr(72), //		userid: String,
                  arr(73).toIntPlus, //		iptype: Int,
                  arr(74).toDoublePlus, //		initbidprice: Double,
                  arr(75).toDoublePlus, //		adpayment: Double,
                  arr(76).toDoublePlus, //		agentrate: Double,
                  arr(77).toDoublePlus, //		lomarkrate: Double,
                  arr(78).toDoublePlus, //		adxrate: Double,
                  arr(79), //		title: String,
                  arr(80), //		keywords: String,
                  arr(81), //		tagid: String,
                  arr(82), //		callbackdate: String,
                  arr(83), //		channelid: String,
                  arr(84).toIntPlus //		mediatype: Int
              )
          })


        // 5 将结果存储到本地磁盘
        val dataFrame = sQLContext.createDataFrame(dataRow, SchemaUtils.logStructType)



        dataFrame.write.parquet(resultOutputPath)
        // 6 关闭sc
        sc.stop()
    }

}
