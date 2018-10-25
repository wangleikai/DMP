package com.dmp.beans

/**
  * @author WangLeiKai
  *         2018/10/24  10:06
  */
class Log (
  val sessionid: String,
  val advertisersid: Int,
  val adorderid: Int,
  val adcreativeid: Int,
  val adplatformproviderid: Int,
  val sdkversion: String,
  val adplatformkey: String,
  val putinmodeltype: Int,
  val requestmode: Int,
  val adprice: Double,
  val adppprice: Double,
  val requestdate: String,
  val ip: String,
  val appid: String,
  val appname: String,
  val uuid: String,
  val device: String,
  val client: Int,
  val osversion: String,
  val density: String,
  val pw: Int,
  val ph: Int,
  val long: String,
  val lat: String,
  val provincename: String,
  val cityname: String,
  val ispid: Int,
  val ispname: String,
  val networkmannerid: Int,
  val networkmannername: String,
  val iseffective: Int,
  val isbilling: Int,
  val adspacetype: Int,
  val adspacetypename: String,
  val devicetype: Int,
  val processnode: Int,
  val apptype: Int,
  val district: String,
  val paymode: Int,
  val isbid: Int,
  val bidprice: Double,
  val winprice: Double,
  val iswin: Int,
  val cur: String,
  val rate: Double,
  val cnywinprice: Double,
  val imei: String,
  val mac: String,
  val idfa: String,
  val openudid: String,
  val androidid: String,
  val rtbprovince: String,
  val rtbcity: String,
  val rtbdistrict: String,
  val rtbstreet: String,
  val storeurl: String,
  val realip: String,
  val isqualityapp: Int,
  val bidfloor: Double,
  val aw: Int,
  val ah: Int,
  val imeimd5: String,
  val macmd5: String,
  val idfamd5: String,
  val openudidmd5: String,
  val androididmd5: String,
  val imeisha1: String,
  val macsha1: String,
  val idfasha1: String,
  val openudidsha1: String,
  val androididsha1: String,
  val uuidunknow: String,
  val userid: String,
  val iptype: Int,
  val initbidprice: Double,
  val adpayment: Double,
  val agentrate: Double,
  val lomarkrate: Double,
  val adxrate: Double,
  val title: String,
  val keywords: String,
  val tagid: String,
  val callbackdate: String,
  val channelid: String,
  val mediatype: Int
          ) extends Product with Serializable{
  /**
    * 通过角标的方式访问类的属性
    * @param n
    * @return
    */
  override def productElement(n: Int): Any = n match {
    case 0 => sessionid
    case 1 => advertisersid
    case 2 => adorderid
    case 3 => adcreativeid
    case 4 => adplatformproviderid
    case 5 => sdkversion
    case 6 => adplatformkey
    case 7 => putinmodeltype
    case 8 => requestmode
    case 9 => adprice
    case 10 => adppprice
    case 11 => requestdate
    case 12 => ip
    case 13 => appid
    case 14 => appname
    case 15 => uuid
    case 16 => device
    case 17 => client
    case 18 => osversion
    case 19 => density
    case 20 => pw
    case 21 => ph
    case 22 => long
    case 23 => lat
    case 24 => provincename
    case 25 => cityname
    case 26 => ispid
    case 27 => ispname
    case 28 => networkmannerid
    case 29 => networkmannername
    case 30 => iseffective
    case 31 => isbilling
    case 32 => adspacetype
    case 33 => adspacetypename
    case 34 => devicetype
    case 35 => processnode
    case 36 => apptype
    case 37 => district
    case 38 => paymode
    case 39 => isbid
    case 40 => bidprice
    case 41 => winprice
    case 42 => iswin
    case 43 => cur
    case 44 => rate
    case 45 => cnywinprice
    case 46 => imei
    case 47 => mac
    case 48 => idfa
    case 49 => openudid
    case 50 => androidid
    case 51 => rtbprovince
    case 52 => rtbcity
    case 53 => rtbdistrict
    case 54 => rtbstreet
    case 55 => storeurl
    case 56 => realip
    case 57 => isqualityapp
    case 58 => bidfloor
    case 59 => aw
    case 60 => ah
    case 61 => imeimd5
    case 62 => macmd5
    case 63 => idfamd5
    case 64 => openudidmd5
    case 65 => androididmd5
    case 66 => imeisha1
    case 67 => macsha1
    case 68 => idfasha1
    case 69 => openudidsha1
    case 70 => androididsha1
    case 71 => uuidunknow
    case 72 => userid
    case 73 => iptype
    case 74 => initbidprice
    case 75 => adpayment
    case 76 => agentrate
    case 77 => lomarkrate
    case 78 => adxrate
    case 79 => title
    case 80 => keywords
    case 81 => tagid
    case 82 => callbackdate
    case 83 => channelid
    case 84 => mediatype
  }

  /**
    * 类中有多少个成员变量
    * @return
    */
  override def productArity: Int = 85

  /**
    *
    * @param that
    * @return
    */
  override def canEqual(that: Any): Boolean = that.isInstanceOf[Log]
}

object Log{

  import com.dmp.utils.StrUtils._
  def apply(arr:Array[String]):Log = new Log(
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
}

