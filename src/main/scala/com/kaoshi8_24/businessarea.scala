package src.main.scala.com.kaoshi8_24

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 商圈
  */
object businessarea {

  def main(args: Array[String]): Unit = {
//    if(args.length != 4){
//      println("目录不匹配,退出程序")
//      sys.exit()
//    }
//    val Array(inputPath,appPath ,stopPath,devicePath )=args
    //创建上下文
val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val log: RDD[String] = sc.textFile("D://dataSource/json.txt")
   // val logs: mutable.Buffer[String] = log.collect().toBuffer
    val rdd1: RDD[List[(String, Int)]] = log.map(x => {
      var list: List[(String, Int)] = List()
      //解析json
      val jsonparse: JSONObject = JSON.parseObject(x)
      //判断状态是否成功
      val status = jsonparse.getIntValue("status")
//      if (status == 0) return ""
      // 接下来解析内部json串,判断每个key的valus都不为空
      val regeocodeJson = jsonparse.getJSONObject("regeocode")
//      if (regeocodeJson == null || regeocodeJson.keySet().isEmpty) return ""

      val poisArray = regeocodeJson.getJSONArray("pois")
//      if (poisArray == null || poisArray.isEmpty) return null

      // 创建集合 保存数据
      val buffer = collection.mutable.ListBuffer[String]()

      for (item <- poisArray.toArray) {
        if (item.isInstanceOf[JSONObject]) {
          val json = item.asInstanceOf[JSONObject]
          val ba: String = json.getString("businessarea")
          if (ba != "[]" && ba != "") {
            list :+= (ba, 1)
          }
        }
      }
      list
    })
    val res: RDD[(String, Int)] = rdd1.flatMap(x=>x).reduceByKey(_+_)
   res.collect().foreach(println)
    //var list: List[List[String]] = List()
//   var list:List[List[String]]= List()
//    for (i <- 0 until logs.length) {
//      val jsonstr: String = logs(i).toString
//
//      //解析json
//      val jsonparse: JSONObject = JSON.parseObject(jsonstr)
//      //判断状态是否成功
//      val status = jsonparse.getIntValue("status")
//      if (status == 0) return ""
//      // 接下来解析内部json串,判断每个key的valus都不为空
//      val regeocodeJson = jsonparse.getJSONObject("regeocode")
//      if (regeocodeJson == null || regeocodeJson.keySet().isEmpty) return ""
//
//      val poisArray = regeocodeJson.getJSONArray("pois")
//      if (poisArray == null || poisArray.isEmpty) return null
//
//      // 创建集合 保存数据
//      val buffer = collection.mutable.ListBuffer[String]()
//
//      for(item <- poisArray.toArray){
//        if(item.isInstanceOf[JSONObject]){
//          val json = item.asInstanceOf[JSONObject]
//          buffer.append(json.getString("businessarea"))
//        }
//      }
//      val list1: List[String] = buffer.toList
//      list:+=list1
//    }
//     // list.foreach(println)
//    val stringToInt: Map[String, Int] = list.flatMap(x => x)
//      .map(x => (x, 1))
//      .groupBy(x => x._1)
//      .mapValues(x => x.size)
//
//    stringToInt
  }
}

