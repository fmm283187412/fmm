package src.main.scala.com.Tags


import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import src.main.scala.com.utils.Tag

/**
  * 5)	关键字（标签格式：Kxxx->1）xxx 为关键字，关键字个数不能少于 3 个字符，且不能
  * 超过 8 个字符；关键字中如包含‘‘|’’，则分割成数组，转化成多个关键字标签
  */
object TagsKeywords extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val stopword: Broadcast[Map[String, String]] = args(1).asInstanceOf[Broadcast[Map[String, String]]]
    // 获取关键字，打标签
     val kwds: Array[String] = row.getAs[String]("keywords").split("\\|")
    // 按照过滤条件进行过滤数据
    kwds.filter(word=>{
      word.length>=3 && word.length <=8 && !stopword.value.contains(word)
    })
      .foreach(word=>list:+=("K"+word,1))
      list
    }

}
