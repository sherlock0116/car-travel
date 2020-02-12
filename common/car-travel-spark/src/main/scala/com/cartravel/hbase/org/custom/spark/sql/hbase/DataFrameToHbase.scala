package com.cartravel.hbase.org.custom.spark.sql.hbase

import com.cartravel.utils.GlobalConfigUtils
import org.apache.spark.sql.DataFrame

/**
  * Created by angel
  */
object DataFrameToHbase {

  lazy val save = (result:DataFrame , tableName:String , rowkey:String , regionNum:Int ,bulkload:Boolean ) => {
    result.write.format(GlobalConfigUtils.getProp("custom.hbase.path"))
      .options(Map(
        "hbase.table.name" -> tableName ,
        "hbase.table.rowkey" -> rowkey ,
        "hbase.table.columnFamily" -> "MM" ,
        "hbase.table.regionNum" -> s"${regionNum}" ,
        "hbase.engable.bulkload" -> s"${bulkload}"
      )).save()
  }
}
