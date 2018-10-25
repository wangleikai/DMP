package com.dmp.utils

/**
  * @author WangLeiKai
  *         2018/10/24  9:26
  */
class StrUtils(val str:String) {

  /**
    * 扩展toInt方法，如果字符串为空，则返回0
    * @return
    */
  def toIntPlus = try{
    str.toInt
  }catch {
    case _: Exception => 0
  }

  /**
    * 扩展toDouble方法，如果字符串为空，则返回0.0
    * @return
    */
  def toDoublePlus  = try{
    str.toDouble
  }catch {
    case _: Exception => 0d
  }
}

object StrUtils{
  implicit def StrUtils(str:String) = new StrUtils(str)
}