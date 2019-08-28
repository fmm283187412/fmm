package scala.com.requirement



import java.io.{FileInputStream, InputStream}
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

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
object proCity {
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
    // 注册临时表
    df.registerTempTable("ad")
    // 指标统计
    val res: DataFrame = sQLContext.sql("select count(1) ct ,provincename,cityname  from ad group by provincename,cityname order by provincename ")
    //res.write.mode("overwrite").partitionBy("provincename", "cityname").json("D://result2019-8-20")
   // res.coalesce(1).write.mode("overwrite").partitionBy("provincename", "cityname").json(outputPath)
    /*
    持久化数据到mysql
     */
//    val prop = new Properties()
//    val path = Thread.currentThread().getContextClassLoader.getResource("mysql.properties").getPath
//    prop.load(new FileInputStream(path))
//    val url = prop.getProperty("url")
//    val table = prop.getProperty("table")
//    val propConect = new Properties()
//    propConect.put("user",prop.getProperty("user"))
//    propConect.put("password",prop.getProperty("password"))
//    res.write.mode("overwrite").jdbc(url,table,propConect)
  //加载配置文件,需要使用对应的依赖
    val load: Config = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))
    res.write.mode("overwrite").jdbc(load.getString("jdbc.url"),load.getString("jdbc.TableName"),prop)

    sc.stop()
  }

}
