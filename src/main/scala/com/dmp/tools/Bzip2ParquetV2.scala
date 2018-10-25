package com.dmp.tools


import com.dmp.beans.Log
import com.dmp.utils.SchemaUtils
import org.apache.commons.io.filefilter.FileFilterUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将原始日志文件转换成parquet文件格式
  * 采用snappy压缩格式
  */
object Bzip2ParquetV2 {

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
        val dataRow: RDD[Log] = rawdata
          .map(line => line.split(",", line.length))
          .filter(_.length >= 85)
          .map(Log(_)
          )


        // 5 将结果存储到本地磁盘
        val dataFrame = sQLContext.createDataFrame(dataRow)

        // 判断结果存储路径是否存在，如果存在则删除
        val hadoopConfiguration = sc.hadoopConfiguration
        val fs = FileSystem.get(hadoopConfiguration)

        val resultPath = new Path(resultOutputPath)
        if(fs.exists(resultPath)) {
            fs.delete(resultPath, true)
        }


        dataFrame.write.parquet(resultOutputPath)
        // 6 关闭sc
        sc.stop()
    }

}
