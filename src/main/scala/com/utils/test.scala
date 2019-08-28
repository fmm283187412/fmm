package src.main.scala.com.utils
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import src.main.scala.com.Tags.TagsBusiness
/**
  * 测试类
  */
object test {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
   val ssc = new SQLContext(sc)
    val df: DataFrame = ssc.read.parquet("D:\\out123")
    df.map(row =>{
      val business = TagsBusiness.makeTags(row)
      business
    }).foreach(println)

  }
}
