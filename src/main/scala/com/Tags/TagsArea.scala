package src.main.scala.com.Tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import src.main.scala.com.utils.Tag

/**
  * 	地域标签（省标签格式：ZPxxx->1, 地市标签格式: ZCxxx->1）xxx 为省或市名称
  */
object TagsArea extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val provincename = row.getAs[String]("provincename")
    val cityname = row.getAs[String]("cityname")
    //provincename 设备所在省份名称
    if(StringUtils.isNotBlank(provincename)){
      list :+= ("ZP" + provincename,1)
    }
    //设备所在城市名称
    if(StringUtils.isNotBlank(cityname)){
      list :+= ("ZC" + cityname,1)
    }

      list
  }
}
