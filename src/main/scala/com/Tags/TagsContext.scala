package src.main.scala.com.Tags


import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import src.main.scala.com.utils.TagUtils

/**
  * 上下文标签
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
    if(args.length != 5){
      println("目录不匹配,退出程序")
      sys.exit()
    }
    val Array(inputPath,appPath ,stopPath,devicePath,days )=args
    //创建上下文
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    //todo 调用Hbase API
    //加载配置文件
    val load: Config = ConfigFactory.load()
    val hbaseTableName: String = load.getString("hbase.TableName")
    //创建Hadoop任务
    val configuration: Configuration = sc.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum",load.getString("hbase.host"))
    // 创建HbaseConnection
    val hbconn: Connection = ConnectionFactory.createConnection(configuration)
    val hbadmin: Admin = hbconn.getAdmin
    //判断表是否可用
    if(!hbadmin.tableExists(TableName.valueOf(hbaseTableName))){
      //创建表操作
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      val descriptor = new HColumnDescriptor("tags")
      tableDescriptor.addFamily(descriptor)
      hbadmin.createTable(tableDescriptor)
      hbadmin.close()
      hbconn.close()
    }
    //创建JobConf
    val jobconf = new JobConf(configuration)
    //指定输出类型和表
    jobconf.setOutputFormat(classOf[TableOutputFormat])
    jobconf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTableName)
//    val appMap: Map[String, String] = sc.textFile(appPath).filter(_.size>4).flatMap(line => {
//      import scala.collection.mutable.Map
//      val map = Map[String, String]()
//      val fields: Array[String] = line.split("\t")
//      map += (fields(4) -> fields(1))
//      map
//    }).collect().toMap
//    //println(appMap)
//    val broadcastAppMap = sc.broadcast(appMap)
    //读取数据
    val df: DataFrame = sQLContext.read.parquet(inputPath)
    //读取字典文件
    val map: Map[String, String] = sc.textFile(appPath).map(_.split("\t", -1))
      .filter(_.length >= 5).map(arr => (arr(4), arr(1))).collect().toMap

    //将处理好的数据广播
    val broadcastAppMap: Broadcast[Map[String, String]] = sc.broadcast(map)
    // 获取停用词库
    val stopword = sc.textFile(stopPath).map((_,0)).collect().toMap
    val bcstopword = sc.broadcast(stopword)
    //生成设备映射广播变量
    val deviceMap: Map[String, String] = sc.textFile(devicePath).map(line => {
      var list = List[(String, String)]()
      val fields = line.split("\t")
      if (fields.length > 1) {
        list :+= (fields(0), fields(1))
      }
      list
    }).collect.flatten.toMap

    val deviceMapBroadcast = sc.broadcast(deviceMap)
    //过滤符合Id的数据
    df.filter(TagUtils.OneUserId)
    //接下来所有的标签都在内部实现
      .map(row => {
      //取出用户Id
      val userId = TagUtils.getOneUserId(row)
      //接下来通过row数据打上所有标签(按照要求)
      val adList = TagsAd.makeTags(row)
      //appname标签
      val appList: List[(String, Int)] = TagsApp.makeTags(row,broadcastAppMap.value)
      //渠道标签
      val cnList: List[(String, Int)] = TagsCN.makeTags(row)
      //设备：操作系统|联网方式|运营商 打标签
      val deviceList: List[(String, Int)] = TagsDevice.makeTags(row,deviceMapBroadcast.value)

      //	关键字
      val keywordList: List[(String, Int)] = TagsKeywords.makeTags(row,bcstopword)
      //	地域标签
      val areaList: List[(String, Int)] = TagsArea.makeTags(row)
      //商圈标签
      val businessList: List[(String, Int)] = TagsBusiness.makeTags(row)
      ////getNotEmptyID(row)获取到一个ID，返回值是个Option，
      // 有可能有，有可能没有，如果没有打出来的标签是没有意义的，给个默认值""
      (userId,(adList ++ appList ++ cnList ++ deviceList ++ keywordList ++areaList++businessList))

    }).filter(!_._1.toString.equals(""))
      .reduceByKey((list1,list2)=>
        // List(("lN插屏",1),("LN全屏",1),("ZC沈阳",1),("ZP河北",1)....)
        (list1:::list2)
          // List(("APP爱奇艺",List()))
          .groupBy(_._1)
          .mapValues(_.foldLeft[Int](0)(_+_._2))
          .toList
      ).map{
      case(userid,userTag)=>{

        val put = new Put(Bytes.toBytes(userid))
        // 处理下标签
        val tags = userTag.map(t=>t._1+","+t._2).mkString(",")
        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(s"$days"),Bytes.toBytes(tags))
        (new ImmutableBytesWritable(),put)
      }
    }
      // 保存到对应表中
      .saveAsHadoopDataset(jobconf)

    sc.stop()
  }
}
