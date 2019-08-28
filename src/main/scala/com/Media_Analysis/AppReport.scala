package src.main.scala.com.Media_Analysis


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import src.main.scala.com.utils.RptUtils

/**
  * 媒体分析
  */
object AppReport {
  def main(args: Array[String]): Unit = {
    /**
      * 1) 判断参数个数
      */
/// 判断路径是否正确
    if(args.length != 3){
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    /**
      * 2）接收参数
      */
    val Array(inputPath, appMappingPath,outputPath) = args
    /**
      * 3) 初始化程序入口
      */
    val conf = new SparkConf()
    conf.setAppName(this.getClass.getName)
    conf.setMaster("local[*]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    /**
      * 4) 把APP映射文件作为广播变量
      * 数据格式：
      * 1	乐自游	A06		cn.net.inch.android	通过GPS的定为实现景区的自动语音讲解的功能。
      * 可能出现的原因：APP名字可能会产生变化，但是APPID号不会变化是唯一的，所以需要映射
      */
    val appMap: Map[String, String] = sc.textFile(appMappingPath).filter(_.size>4).flatMap(line => {
      import scala.collection.mutable.Map
      val map = Map[String, String]()
      val fields: Array[String] = line.split("\t")
      map += (fields(4) -> fields(1))
      map
    }).collect().toMap
//println(appMap)
    val broadcastAppMap = sc.broadcast(appMap)

    /**
      * 5) 生成报表
      */
    // 获取数据
    val df = sQLContext.read.parquet(inputPath)
    // 将数据进行处理，统计各个指标
    df.map(row => {
      //把需要的字段全部取到
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val WinPrice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      val appid = row.getAs[String]("appid")
      val appname = row.getAs[String]("appname")
      //统计的媒体APP，.value就获取到值了。Map里面有getOrElse功能，拿着appid去获取映射里面的值，如果能获取到就用这个值，如果获取不到就使用appname
      val appName = broadcastAppMap.value.getOrElse(appid, appname)

        (appName, (RptUtils.request(requestmode, processnode) ++ RptUtils.click(requestmode, iseffective) ++ RptUtils.Ad(iseffective, isbilling, isbid, iswin,
          adorderid, WinPrice, adpayment)))

      //List(1,1) ++ List(0,0)  => List(1,1,0,0)

    }).filter(tuple => {
      tuple._1.nonEmpty && !"".equals(tuple._1)
      tuple._1 == "爱奇艺" || tuple._1 == "腾讯新闻" || tuple._1 == "PPTV"

    }).reduceByKey {
      case (list1, list2) => {
        //List(1,0) .zip List(2,3)  => List((1,2),(0,3))
        list1.zip(list2).map {
          case (x, y) => x + y
        }
      }
    }.map(t=>{
      t._1+","+t._2.mkString(",")
    }).coalesce(1).saveAsTextFile(outputPath)




    sc.stop()

  }


}
