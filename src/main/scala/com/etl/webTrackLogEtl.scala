package com.etl

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.json4s.JsonAST.{JNothing, JString}
import org.json4s.jackson.JsonMethods._

object webTrackLogEtl {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val column_list = List("site_id", "created_at", "cookie_id", "domain", "url_path", "url_params", "operating_system",
    "operating_system_version", "browser", "browser_version", "ip", "uid", "type", "element_id", "extra")

  case class buriedPointData(
                              site_id: String,
                              created_at: String,
                              cookie_id: String,
                              domain: String,
                              url_path: String,
                              url_params: String,
                              operating_system: String,
                              operating_system_version: String,
                              browser: String,
                              browser_version: String,
                              ip: String,
                              uid: String,
                              `type`: String,
                              element_id: String,
                              extra: String
                            )
  def getNowDate():String={
    /*
    * return Today(yyyy-MM-dd)*/
    val now:Date = new Date()
    val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateFormat.format( now )
    today
  }
  def getYesterday():String= {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val yesterday = dateFormat.format(cal.getTime())
    yesterday
  }

  def process_line(line: String): List[String] = {
    val json_res = parse(line.split("\t",3)(2).replace("\\\"", "\"").replace("\"{\"", "{\"").replace("\"}\"", "\"}")).\("message")
    var value = "NULL"
    var res = List[String]()
    for (elem <- column_list) {
      if (json_res.\(elem) != JNothing) {
        if (elem == "extra") {
          value = if (json_res.\(elem) == JString("")) {
            json_res.\(elem).values.toString
          } else {
            compact(render(json_res.\(elem)))
          }
        } else {
          value = json_res.\(elem).values.toString
        }
      } else {
        value = "NULL"
      }
      res = res :+ value
    }
    res
  }


  def main(args: Array[String]): Unit = {
    val Debug = false

    if (Debug) {
      // 本地测试
      val now:Date = new Date()
      val now_timestamp = now.getTime
      val sc = SparkSession.builder().master("local").getOrCreate().sparkContext
      val fileRdd = sc.textFile("./src/main/scala/com/etl/webTrackLogTestFile")
      fileRdd.mapPartitions(partition => {
        partition.map(line => {
          process_line(line)
        })
      }).saveAsTextFile("./src/main/scala/com/etl/webTrackLogTestFile_res_" + now_timestamp)
    } else {
      // 线上
      val spark = SparkSession.builder().appName("webTrackLog").enableHiveSupport().getOrCreate()
      val sqlContext = spark.sqlContext
      import sqlContext.implicits._

      val sc = spark.sparkContext
      val yesterday = getYesterday()
      val fileRdd = sc.textFile(s"hdfs://hadoop-cluster/log/app_log/beeper_bi_collect/custom/${yesterday.replace("-","")}")
      fileRdd.mapPartitions(partition => {
        partition.map(line => {
          val e = process_line(line)
          buriedPointData(e.head,e(1),e(2),e(3),e(4),e(5),e(6),e(7),e(8),e(9),e(10),e(11),e(12),e(13),e(14))
        })
      }).toDF().write
        .mode(SaveMode.Overwrite)
        .format("parquet")
        .saveAsTable("tmp.tmp_web_track_log")
      sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
      sqlContext.sql("insert overwrite table yn_log.web_track_log partition(site_id, p_day) " +
        "select created_at,cookie_id,domain,url_path,url_params,operating_system,operating_system_version,browser,browser_version," +
        s"ip,uid,type,element_id,extra,site_id as site_id, '$yesterday' as p_day from tmp.tmp_web_track_log")
    }
  }
}