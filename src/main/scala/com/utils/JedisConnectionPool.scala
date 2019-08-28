package src.main.scala.com.utils
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
object JedisConnectionPool {
  //连接配置
  val config= new JedisPoolConfig
  config.setMaxWaitMillis(20000)
  //最大连接数
  config.setMaxTotal(20)
  //最大空闲连接数
  config.setMaxIdle(10)
  //设置连接池属性分别有： 配置  主机名   端口号  连接超时时间    Redis密码
  val pool=new JedisPool(config,"192.168.236.100")
  //连接池
  def getConnections(): Jedis ={
    pool.getResource
  }
//调用val jedis = JedisConnectionPool.getConnections()

}
