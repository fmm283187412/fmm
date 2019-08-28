package src.main.scala.com.Rpt

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import src.main.scala.com.utils.{ConnectPool, RptUtils}

/**
  * 地域分布指标
  */
object LocationRpt {
  def main(args: Array[String]): Unit = {
    // 判断路径是否正确
    if(args.length != 2){
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    // 创建一个集合保存输入和输出目录
    val Array(inputPath,outputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    // 创建执行入口
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    // 获取数据
    val df = sQLContext.read.parquet(inputPath)
    // 将数据进行处理，统计各个指标
    val res: RDD[String] = df.map(row => {
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
      //key值  是地域的省市
      val pro = row.getAs[String]("provincename")
      val city = row.getAs[String]("cityname")
      //创建三个对应的方法处理九个指标

      ((pro, city), (RptUtils.request(requestmode, processnode) ++ RptUtils.click(requestmode, iseffective) ++ RptUtils.Ad(iseffective, isbilling, isbid, iswin,
        adorderid, WinPrice, adpayment)))
    }).reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    }) //整理元素
      .map(t => {
      t._1 + "," + t._2.mkString(",")
    })
    //res.foreachPartition(insertData())
    res.collect().foreach(println)

    sc.stop()

  }
  //如果存入mysql的话,需要使用foreachPartition
  //需要自己写一个连接池
  def insertData(iterator: Iterator[(String,List[Double])])={
    Class.forName("com.mysql.jdbc.Driver").newInstance()
    val connection = ConnectPool.getConnection
    iterator.foreach(data=>{
      val ps = connection.prepareStatement("insert into LocationRpt(clientName,requestmode,processnode,iseffective,isbilling,isbid,iswin,adorderid,winprice,adpayment) " +
        "values (?,?,?,?,?,?,?,?,?,?)")
      ps.setString(1,data._1)
      ps.setDouble(2,data._2(0))
      ps.setDouble(3,data._2(1))
      ps.setDouble(4,data._2(2))
      ps.setDouble(5,data._2(3))
      ps.setDouble(6,data._2(4))
      ps.setDouble(7,data._2(5))
      ps.setDouble(8,data._2(6))
      ps.setDouble(9,data._2(7))
      ps.setDouble(10,data._2(8))
      ps.executeLargeUpdate()
    })


  }

}
