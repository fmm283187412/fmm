package src.main.scala.com.kaoshi8_24
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.collection.mutable

object Tags {

  def main(args: Array[String]): Unit = {
    var list: List[String] = List()
    //初始化环境
    val conf = new SparkConf().setAppName(this.getClass.getName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    //读取文件
    val log: RDD[String] = sc.textFile("D://dataSource/json.txt")

    val logs: mutable.Buffer[String] = log.collect().toBuffer
    //logs.foreach(println)
    for(i <- 0 until logs.length) {
      val str: String = logs(i).toString

      val jsonparse: JSONObject = JSON.parseObject(str)
     //判断状态是否成功
      val status = jsonparse.getIntValue("status")
      if (status == 0) return ""

      val regeocodeJson = jsonparse.getJSONObject("regeocode")
      if (regeocodeJson == null || regeocodeJson.keySet().isEmpty) return ""

      val poisArray = regeocodeJson.getJSONArray("pois")
      if (poisArray == null || poisArray.isEmpty) return null

      // 创建集合 保存数据
    val buffer = collection.mutable.ListBuffer[String]()
    // 循环输出
    for (item <- poisArray.toArray) {
     if (item.isInstanceOf[JSONObject]) {
      val json = item.asInstanceOf[JSONObject]
     buffer.append(json.getString("type"))
   }
  }

  list:+=buffer.mkString(";")
  }
    //list.foreach(println)
    val stringToInt: Map[String, Int] = list.flatMap(x => x.split(";"))
      .map(x => (x, 1))
      .groupBy(x => x._1)
      .mapValues(x => x.length)

    stringToInt.foreach(println)
  }
}
