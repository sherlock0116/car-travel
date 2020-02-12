import com.cartravel.spark.SparkEngine

object ScalaTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = SparkEngine.getSparkConf()
    val session = SparkEngine.getSparkSession(sparkConf)
    session.read.parquet("/test_parquet/tbDate").createOrReplaceTempView("tbDate")
    session.read.parquet("/test_parquet/tbStock").createOrReplaceTempView("tbStock")
    session.read.parquet("/test_parquet/tbStockDetail").createOrReplaceTempView("tbStockDetail")
    session.sql("select * from tbDate").show()
    session.sql("select * from tbStock").show()
    session.sql("select * from tbStockDetail").show()


//    session.sql(
//      """
//        |select
//        |a.ordernumber,
//        |10+20+30+ sum(b.amout) as sumofamount
//        |from
//        |tbstock a,tbstockdetail b
//        |where a.ordernumber = b.ordernumber
//        |group by a.ordernumber
//        |having sumofamount >1000
//      """.stripMargin).explain(true)


    session.sql(
      """
        |select
        |c.theyear,
        |c.thequot,
        |1000+2000 + sum(b.amout) as sumofamount
        |from
        |tbStock a,tbStockDetail b,tbDate c
        |where
        |a.ordernumber=b.ordernumber
        |and
        |a.dataID=c.dataID
        |group by c.theyear,c.thequot
        |order by sumofamount
        |desc limit 10
      """.stripMargin).explain(true)
  }
}
