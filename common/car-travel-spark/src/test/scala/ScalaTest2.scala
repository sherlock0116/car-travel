import java.io.OutputStream
import java.net.InetSocketAddress
import java.util.concurrent.Executors

import com.cartravel.loggings.Logging
import com.cartravel.spark.SparkEngine
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.sun.net.httpserver.{Headers, HttpExchange, HttpHandler, HttpServer}
/**
  * Created by angel
  */
object ScalaTest2 extends Logging{
//  def main(args: Array[String]): Unit = {
//    val sparkConf = SparkEngine.getSparkConf()
//    val spark = SparkEngine.getSparkSession(sparkConf)
//    spark.read.json("file:///Users/angel/Desktop/pmt.json").createOrReplaceTempView("table_a")
////    spark.read.json("/pmt.json").createOrReplaceTempView("table_a")
//    spark.sqlContext.cacheTable("table_a")
//    spark.sql(
//      """
//        |select
//        |  if(req_netease_user is null, 'all', req_netease_user) as req_netease_user,
//        |  if(adorderid is null, 'all', adorderid) as adorderid,
//        |  if(render_name is null,'all', render_name) as render_name,
//        |  if(putinmodeltype is null, 'all', putinmodeltype) as putinmodeltype,
//        |  count(distinct adorderid) as bid_request_num,
//        |  count(distinct deviceid) as bid_request_uv,
//        |  count(distinct case when bid_response_nbr='移动' then bid_response_nbr else null end) as offer_num,
//        |  count(distinct case when bid_response_id='移动' then deviceid else null end) as offer_uv,
//        |  requestdate
//        |from
//        |(
//        |select
//        |  distinct requestdate,
//        |  if(ip is null, 'null', ip) as req_netease_user,
//        |  if(appname is null, 'null', appname) as render_name,
//        |  if(putinmodeltype is null,'null', putinmodeltype) as putinmodeltype,
//        |  if(adorderid is null, 'null', adorderid) as campaign_id,
//        |  if(adorderid is null, 'null', adorderid) as spec_id,
//        |  if(appname is null, 'null', appname) as app_bundle,
//        |  adorderid,
//        |  ispname  as bid_response_nbr,
//        |  ispname as bid_response_id,
//        |  appid as deviceid
//        |from table_a where requestdate = '2018-10-07' and adorderid is not null
//        |) tmp group by requestdate, req_netease_user, adorderid,spec_id, campaign_id, render_name, app_bundle,putinmodeltype
//        |grouping sets(
//        |  (requestdate),
//        |  (requestdate, req_netease_user),
//        |  (requestdate, adorderid),
//        |  (requestdate, render_name),
//        |  (requestdate, putinmodeltype),
//        |  (requestdate, req_netease_user, campaign_id),
//        |  (requestdate, req_netease_user, spec_id),
//        |  (requestdate, req_netease_user, render_name),
//        |  (requestdate, req_netease_user, putinmodeltype),
//        |  (requestdate, req_netease_user, adorderid, adorderid),
//        |  (requestdate, req_netease_user, adorderid, render_name),
//        |  (requestdate, req_netease_user, adorderid, putinmodeltype),
//        |  (requestdate, req_netease_user, adorderid, adorderid, render_name),
//        |  (requestdate, req_netease_user, adorderid, adorderid, render_name, render_name),
//        |  (requestdate, req_netease_user, adorderid, adorderid, render_name, putinmodeltype),
//        |  (requestdate, req_netease_user, adorderid, adorderid, render_name, app_bundle, putinmodeltype)
//        |)
//      """.stripMargin).show()
//

  def main(args: Array[String]) {

    //启动Spark Application准备
    val sparkConf = SparkEngine.getSparkConf()
    val spark = SparkEngine.getSparkSession(sparkConf)
    val sparkContext = spark.sparkContext

    val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)

    //创建HttpServer
    createServer(9999, spark, fileSystem)

    Runtime.getRuntime.addShutdownHook(
      new Thread {
        override def run(): Unit = {
          println("shutdown hook start run...")
        }
      })
  }

  /**
    * 创建ConvertServer
    * @param port 端口
    */
  def createServer(port: Int, session: SparkSession, fileSystem:FileSystem): Unit = {

    val httpServer = HttpServer.create(new InetSocketAddress(port), 30)
    httpServer.setExecutor(Executors.newCachedThreadPool())
    httpServer.createContext("/doSomething", new HttpHandler() {
      override def handle(httpExchange: HttpExchange): Unit = {
        System.out.println("处理新请求:" + httpExchange.getRequestMethod + " , " + httpExchange.getRequestURI)

        var response = "成功"
        var httpCode = 200

        val responseHeaders: Headers = httpExchange.getResponseHeaders
        responseHeaders.set("Content-Type", "text/html;charset=utf-8")

        // 调用SparkSQL的方法进行测试
        try {
          val json: DataFrame = session.read.json("/datatest/")
        } catch {
          case e: Exception =>
            response = e.getMessage
            httpCode = 500
        }

        /**
          * 第一个参数：返回状态码
          * 第二个参数：返回字节数
          */
        httpExchange.sendResponseHeaders(httpCode, response.getBytes.length)
        val responseBody: OutputStream = httpExchange.getResponseBody
        responseBody.write(response.getBytes)
        responseBody.flush
        responseBody.close
      }

      httpServer.createContext("/ping", new HttpHandler() {
        override def handle(httpExchange: HttpExchange): Unit = {
          var response = "存活"
          var httpCode = 200

          try {
            if (session.sparkContext.isStopped) {
              httpCode = 400
              response = "spark终止"
            }
          } catch {
            case e: Exception =>
              httpCode = 500
              response = "服务异常"
          } finally {
            httpExchange.sendResponseHeaders(httpCode, response.getBytes().length)
            val out = httpExchange.getResponseBody //获得输出流
            out.write(response.getBytes())
            out.flush()
            httpExchange.close()
          }
        }
      })


      /**
        * 停止sc测试
        */
      httpServer.createContext("/stop_sc", new HttpHandler() {
        override def handle(httpExchange: HttpExchange): Unit = {
          var response = "成功"
          var httpCode = 200

          try {
            session.sparkContext.stop()
          } catch {
            case e: Exception =>
              httpCode = 500
              response = "服务异常"
          } finally {
            httpExchange.sendResponseHeaders(httpCode, response.getBytes().length)
            val out = httpExchange.getResponseBody //获得输出流
            out.write(response.getBytes())
            out.flush()
            httpExchange.close()
          }
        }
      })

      httpServer.start()
      println("ConvertServer started " + port + " ......")
    })
  }




}
