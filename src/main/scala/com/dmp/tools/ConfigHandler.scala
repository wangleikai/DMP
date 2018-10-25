package com.dmp.tools

import java.util.Properties

import com.typesafe.config.ConfigFactory

/**
  * @author WangLeiKai
  *         2018/10/24  14:59
  */
object ConfigHandler {
  private lazy val config = ConfigFactory.load()

  //parquet文件所在路径
  val parquetPath = config.getString("parquet.path")

  val logdataAnalysisResultJsonPath = config.getString("rpt.logdataAnalysis")

  //关于Mysql的配置
  val driver = config.getString("db.driver")
  val url = config.getString("db.url")
  val user = config.getString("db.user")
  val password = config.getString("db.password")
  val logdataAnalysisTableName = config.getString("db.logdataAnalysis.table")
  val areaAnalysisTableName: String = config.getString("db.rptAreaAnalysis.table")

  val mediaAnalysisTableName: String = config.getString("db.medisAnalysis.table")

  //封装Mysql Props
  val dbProps = new Properties()
  dbProps.setProperty("driver", driver)
  dbProps.setProperty("user", user)
  dbProps.setProperty("password", password)

  val appdictPath: String = config.getString("appdict")
}
