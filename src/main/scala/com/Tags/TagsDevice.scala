package src.main.scala.com.Tags

import org.apache.spark.sql.Row
import src.main.scala.com.utils.Tag

/**
  * 设备：操作系统|联网方式|运营商
  */
object TagsDevice extends Tag {
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]
    val deviceDict: Map[String, String] = args(1).asInstanceOf[Map[String, String]]
    //操作系统标签
    //client 设备类型 （1：android 2：ios 3：wp）如果获取不到就是4类型，4就是其他的
    val os: String = deviceDict.getOrElse(row.getAs[Int]("client").toString, deviceDict.get("4").get)
    list :+= (os, 1)
    //联网方式标签
    val network: String = deviceDict.getOrElse(row.getAs[String]("networkmannername"), deviceDict.get("NETWORKOTHER").get)
    list :+= (network, 1)
    //运营商的标签
    val isp: String = deviceDict.getOrElse(row.getAs[String]("ispname"), deviceDict.get("OPERATOROTHER").get)
    list :+= (isp,1)
    list
  }
}
