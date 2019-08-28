package src.main.scala.com.utils
import org.apache.spark.{SparkConf, SparkContext}
/**
  * 将字段文件数据，存储到redis中
  */
object APp2Jedis {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = new SparkContext(conf)
    // 读取字段文件
    val dict = sc.textFile("D:\\前锋教育教学视频\\22班\\项目day01\\Spark用户画像分析\\app_dict.txt")
    // 读取字段文件
    dict.map(_.split("\t",-1))
      .filter(_.length>=5).foreachPartition(arr=>{
      val jedis = JedisConnectionPool.getConnections()
      arr.foreach(arr=>{
        jedis.hset("fmm",arr(4),arr(1))
      })
      jedis.close()
    }
    )
    sc.stop()
  }
}
