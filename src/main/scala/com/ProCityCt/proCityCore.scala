package src.main.scala.com.requirement

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
/**
  * 要求一：
  * 将统计的结果输出成 json 格式，并输出到磁盘目录。
  * {"ct":943,"provincename":"内蒙古自治区","cityname":"阿拉善盟"}
  * {"ct":578,"provincename":"内蒙古自治区","cityname":"未知"}
  * {"ct":262632,"provincename":"北京市","cityname":"北京市"}
  * {"ct":1583,"provincename":"台湾省","cityname":"未知"}
  * {"ct":53786,"provincename":"吉林省","cityname":"长春市"}
  * {"ct":41311,"provincename":"吉林省","cityname":"吉林市"}
  * {"ct":15158,"provincename":"吉林省","cityname":"延边朝鲜族自治州"}
  */
object proCityCore {
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
    // 设置压缩方式 使用Snappy方式进行压缩
    sQLContext.setConf("spark.sql.parquet.compression.codec","snappy")
    // 读取本地文件
    val df = sQLContext.read.parquet(inputPath)
   val rdd: RDD[Row] = df.rdd
    val rdd1: RDD[((Any, Any), Int)] = rdd.map(x => {
      ((x(24), x(25)), 1)

    })
   val rdd2: RDD[(Any, Iterable[((Any, Any), Int)])] = rdd1.groupBy(_._1)
    val rdd3: RDD[(Any, Int)] = rdd2.mapValues(_.toList.size)
    val rdd4: RDD[(Int, Any)] = rdd3.map(x => {
      (x._2, x._1)
    })


   rdd4.saveAsTextFile(outputPath)




  }
}
