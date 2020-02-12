import com.cartravel.spark.SparkEngine
import org.apache.spark.sql.DataFrame

/**
  * Created by angel
  */
object ScalaTest1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = SparkEngine.getSparkConf()
    val spark = SparkEngine.getSparkSession(sparkConf)
    spark.read.json("file:///Users/angel/Desktop/pmt.json").createOrReplaceTempView("table_a")
//    spark.read.json("/pmt.json").createOrReplaceTempView("table_a")
//    spark.read.json("file:///Users/angel/Desktop/pmt.json")
    spark.sql(
      """
        |select
        |  if(req_netease_user is null, 'all', req_netease_user) as req_netease_user,
        |  if(adorderid is null, 'all', adorderid) as adorderid,
        |  if(render_name is null,'all', render_name) as render_name,
        |  if(putinmodeltype is null, 'all', putinmodeltype) as putinmodeltype,
        |  count(distinct adorderid) as bid_request_num,
        |  count(distinct deviceid) as bid_request_uv,
        |  count(distinct case when bid_response_nbr='移动' then bid_response_nbr else null end) as offer_num,
        |  count(distinct case when bid_response_id='移动' then deviceid else null end) as offer_uv,
        |  requestdate
        |from
        |(
        |select
        |  distinct requestdate,
        |  if(ip is null, 'null', ip) as req_netease_user,
        |  if(appname is null, 'null', appname) as render_name,
        |  if(putinmodeltype is null,'null', putinmodeltype) as putinmodeltype,
        |  if(adorderid is null, 'null', adorderid) as campaign_id,
        |  if(adorderid is null, 'null', adorderid) as spec_id,
        |  if(appname is null, 'null', appname) as app_bundle,
        |  adorderid,
        |  ispname  as bid_response_nbr,
        |  ispname as bid_response_id,
        |  appid as deviceid
        |from table_a where adorderid is not null
        |) tmp group by requestdate, req_netease_user, adorderid,spec_id, campaign_id, render_name, app_bundle,putinmodeltype
        |grouping sets(
        |  (requestdate),
        |  (requestdate, req_netease_user),
        |  (requestdate, adorderid),
        |  (requestdate, render_name),
        |  (requestdate, putinmodeltype),
        |  (requestdate, req_netease_user, campaign_id),
        |  (requestdate, req_netease_user, spec_id),
        |  (requestdate, req_netease_user, render_name),
        |  (requestdate, req_netease_user, putinmodeltype),
        |  (requestdate, req_netease_user, adorderid, adorderid),
        |  (requestdate, req_netease_user, adorderid, render_name),
        |  (requestdate, req_netease_user, adorderid, putinmodeltype),
        |  (requestdate, req_netease_user, adorderid, adorderid, render_name),
        |  (requestdate, req_netease_user, adorderid, adorderid, render_name, render_name),
        |  (requestdate, req_netease_user, adorderid, adorderid, render_name, putinmodeltype),
        |  (requestdate, req_netease_user, adorderid, adorderid, render_name, app_bundle, putinmodeltype)
        |)
      """.stripMargin).explain(true)



//    spark.sqlContext.cacheTable("table_a")
//    spark.sql(
//      """
//        |with query_base as
//        |    (
//        |           select
//        |               requestdate as dt,
//        |               if(ip is null, 'null', ip) as req_netease_user,
//        |               if(appname is null, 'null', appname) as render_name,
//        |               if(putinmodeltype is null,'null', putinmodeltype) as platform,
//        |               if(adorderid is null, 'null', adorderid) as campaign_id,
//        |               if(adorderid is null, 'null', adorderid) as spec_id,
//        |               if(appname is null, 'null', appname) as app_bundle,
//        |               adorderid as request_id,
//        |               if(ispname='移动',1,0) as is_nbr,
//        |               ispname as bid_response_id,
//        |               appid as deviceid
//        |           from table_a  where  adorderid is not null
//        |        )
//        |
//        |select
//        |        request_num.netease_user,
//        |        request_num.campaign_id,
//        |        request_num.spec_id,
//        |        request_num.app_bundle,
//        |        request_num.render_name,
//        |        request_num.platform,
//        |        bid_request_num,
//        |        bid_request_uv,
//        |        offer_num,
//        |        offer_uv,
//        |        request_num.dt
//        |from
//        |(
//        |        select
//        |            dt,
//        |            nvl( req_netease_user,'all' ) as netease_user,
//        |            nvl( campaign_id, 'all') as campaign_id,
//        |            nvl( spec_id , 'all') as spec_id,
//        |            nvl( app_bundle , 'all') as app_bundle,
//        |            nvl( render_name,'all') as render_name,
//        |            nvl( platform, 'all') as platform,
//        |            sum(request_num) as bid_request_num,
//        |            count(distinct deviceid) as bid_request_uv,
//        |            count(distinct case when bid_response_nbr='移动' then bid_response_nbr else null end) as offer_uv
//        |        from
//        |        (
//        |          select
//        |              dt,
//        |              req_netease_user,
//        |              campaign_id,
//        |              spec_id,
//        |              app_bundle,
//        |              render_name,
//        |              platform,
//        |              deviceid,
//        |              count(request_id) as request_num,
//        |              max(bid_response_id) as bid_response_nbr
//        |          from query_base
//        |          where bid_response_id = '移动'
//        |          group by dt, req_netease_user, campaign_id, spec_id, app_bundle, render_name, platform,deviceid
//        |        )tmp group by dt, req_netease_user, campaign_id, spec_id, app_bundle, render_name, platform
//        |        grouping sets(
//        |            (dt),
//        |            (dt, req_netease_user),
//        |            (dt, campaign_id),
//        |            (dt, render_name),
//        |            (dt, platform),
//        |            (dt, req_netease_user, campaign_id),
//        |            (dt, req_netease_user, spec_id),
//        |            (dt, req_netease_user, render_name),
//        |            (dt, req_netease_user, platform),
//        |            (dt, req_netease_user, campaign_id, spec_id),
//        |            (dt, req_netease_user, campaign_id, render_name),
//        |            (dt, req_netease_user, campaign_id, platform),
//        |            (dt, req_netease_user, campaign_id, spec_id, render_name),
//        |            (dt, req_netease_user, campaign_id, spec_id, app_bundle, render_name),
//        |            (dt, req_netease_user, campaign_id, spec_id, app_bundle, platform),
//        |            (dt, req_netease_user, campaign_id, spec_id, app_bundle, render_name, platform)
//        |        )
//        |) request_num join
//        |(
//        |
//        |        select
//        |            dt,
//        |            nvl( req_netease_user,'all' ) as netease_user,
//        |            nvl( campaign_id, 'all') as campaign_id,
//        |            nvl( spec_id , 'all') as spec_id,
//        |            nvl( app_bundle , 'all') as app_bundle,
//        |            nvl( render_name,'all') as render_name,
//        |            nvl( platform, 'all') as platform,
//        |            count(distinct bid_response_id ) offer_num
//        |        from
//        |        (
//        |         select
//        |              dt,
//        |              req_netease_user,
//        |              campaign_id,
//        |              spec_id,
//        |              app_bundle,
//        |              render_name,
//        |              platform,
//        |              bid_response_id
//        |          from query_base where dt = '2018-10-07' and  bid_response_id = '移动'
//        |        )tmp group by dt, req_netease_user, campaign_id, spec_id, app_bundle, render_name, platform
//        |        grouping sets(
//        |            (dt),
//        |            (dt, req_netease_user),
//        |            (dt, campaign_id),
//        |            (dt, render_name),
//        |            (dt, platform),
//        |            (dt, req_netease_user, campaign_id),
//        |            (dt, req_netease_user, spec_id),
//        |            (dt, req_netease_user, render_name),
//        |            (dt, req_netease_user, platform),
//        |            (dt, req_netease_user, campaign_id, spec_id),
//        |            (dt, req_netease_user, campaign_id, render_name),
//        |            (dt, req_netease_user, campaign_id, platform),
//        |            (dt, req_netease_user, campaign_id, spec_id, render_name),
//        |            (dt, req_netease_user, campaign_id, spec_id, platform),
//        |            (dt, req_netease_user, campaign_id, spec_id, app_bundle, platform),
//        |            (dt, req_netease_user, campaign_id, spec_id, app_bundle, render_name, platform)
//        |        )
//        |) request_uv on  request_num.netease_user=request_uv.netease_user
//        |and request_num.render_name=request_uv.render_name
//        |and request_num.campaign_id=request_uv.campaign_id
//        |and request_num.spec_id=request_uv.spec_id
//        |and request_num.app_bundle=request_uv.app_bundle
//        |and request_num.platform=request_uv.platform
//      """.stripMargin).show()
  }
}
