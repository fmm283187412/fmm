package src.main.scala.com.Tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import src.main.scala.com.utils.Tag

/**
  * app标签
  */
object TagsApp extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {

    var list= List[(String,Int)]()
    //处理参数类型
      val row = args(0).asInstanceOf[Row]
      //	获取App 名称（标签格式： APPxxxx->1）xxxx 为 App 名称，使用缓存文件 appname_dict 进行名称转换；APP 爱奇艺->1
      val appDict: Map[String, String] = args(1).asInstanceOf[Map[String,String]]

      val appid: String = row.getAs[String]("appid")

      val appname: String = row.getAs[String]("appname")

      val appName = appDict.getOrElse(appid,appname)

    if(StringUtils.isNotEmpty(appName) && !"".equals(appName)){
      list :+= ("APP" + appName,1)
    }
    list

  }
}
