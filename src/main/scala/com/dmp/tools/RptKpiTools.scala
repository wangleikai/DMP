package com.dmp.tools

import com.dmp.utils.JedisPools
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

/**
  * @author WangLeiKai
  *         2018/10/24  17:04
  */
object RptKpiTools {

  def offLineKpi(row : Row) = {
    val rmode = row.getAs[Int]("requestmode")
    val pnode = row.getAs[Int]("processnode")

    val adRequest = if(rmode == 1 && pnode == 3) List[Double](1,1,1)
    else if (rmode == 1 && pnode >= 2) List[Double](1,1,0)
    else if (rmode == 1 && pnode == 1) List[Double](1,0,0)
    else List[Double](0,0,0)

    val efftv = row.getAs[Int]("iseffective")
    val bill = row.getAs[Int]("isbilling")
    val bid = row.getAs[Int]("isbid")


    val win = row.getAs[Int]("iswin")
    val orderId = row.getAs[Int]("adorderid")

    val adRTB = if(efftv == 1 && bill == 1 && bid == 1 && orderId != 0) List[Double](1,0)
    else if(rmode == 3 && efftv == 1) List[Double](0,1)
    else List[Double](0,0)

    val adShowAndClick = if (rmode == 2 && efftv == 1) List[Double](1,0)
    else if (rmode == 3 && efftv == 1) List[Double](0,1)
    else List[Double](0,0)

    val price = row.getAs[Double]("winprice")

    val payment = row.getAs[Double]("adpayment")

    val adCost = if(efftv == 1 && bill == 1 && win == 1) List[Double](price/1000,payment/1000) else List[Double](0,0)

    adRequest ++ adRTB ++ adShowAndClick ++ adCost
  }



  val notEmptyAppName = (appid:String,appname:String) => {
    if(StringUtils.isEmpty(appname)){
      val jedis = JedisPools.getJedis()
      val appName = jedis.hget("appdict",appid)
      jedis.close()
      appname
    }else appname
  }

}
