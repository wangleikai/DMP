package com.dmp.tools

import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * @author WangLeiKai
  *         2018/10/25  14:39
  */
object MysqlHandler {
  def save2db(resultDF:DataFrame, tblName:String, partition:Int = 1)={
    resultDF.coalesce(partition).write.mode(SaveMode.Overwrite).jdbc(
      ConfigHandler.url,
      tblName,
      ConfigHandler.dbProps
    )
  }
}
