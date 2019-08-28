package src.main.scala.com.Rpt

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

object LocationRptSparksql {
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
    df.registerTempTable("ad")
    val res: DataFrame = sQLContext.sql("select\n" +
      "provincename,\n" +
      "cityname,\n" +
      "count(case when requestmode=1 and processnode>=1 then 1 end) request_num_total,\n" +
      "count(case when requestmode=1 and processnode>=2 then 1 end ) request_num_valid,\n" +
      "count(case when requestmode=1 and processnode=3 then 1 end) request_num_ad,\n" +
      "count(case when iseffective =1 and isbilling=1 and isbid =1 then 1 end) bid_num_participate,\n" +
      "count(case when iseffective =1 and isbilling=1 and iswin=1 and adorderid <>0 then 1 end) bid_num_success,\n" +
      "count(case when requestmode = 2 and iseffective = 1 and isbilling = 1 then 1 end ) ad_num_show,\n" +
      "count(case when requestmode = 3 and iseffective = 1 and isbilling = 1 then 1 end) ad_num_click,\n" +
      "sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice else 0 end )/1000 ad_cost,\n" +
      "sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment else 0 end )/1000 ad_payment\n" +
      "from ad\n" +
      "group by provincename,cityname")
    res.coalesce(1).write.mode("overwrite").partitionBy("provincename", "cityname").json(outputPath)


  }
}
