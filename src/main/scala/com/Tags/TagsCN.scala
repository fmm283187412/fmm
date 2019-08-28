package src.main.scala.com.Tags

/**
  * 3)	渠道（标签格式： CNxxxx->1）xxxx 为渠道 ID(adplatformproviderid)
  */

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import src.main.scala.com.utils.Tag

object TagsCN extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list= List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val channelid = row.getAs[String]("channelid")
    if(StringUtils.isNotBlank(channelid)){
      list :+= ("CN".concat(channelid),1)
    }

    list
  }
}
