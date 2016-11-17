package com.sfexpress.bdp


import com.datastax.spark.connector.cql._
import org.apache.spark.{SparkConf, SparkContext}

case class WQSLog(log_id: Long, oper_time: String, oper_uesr_name: String, oper_userid: String, source_app: String, dest_app: String, dst_ip: String, request_type: String, oper_type: String, resource_details: String, query_obj_info: String, query_obj_type: String, email: String, src_ip: String, src_mac: String, oper_content: String, oper_result: String)

object Spark2Cassandra {
  def main(args: Array[String]) {
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "10.202.34.209,10.202.34.211,10.202.34.212")
      /*.set("spark.cassandra.auth.username", "cassandra")
      .set("spark.cassandra.auth.password", "cassandra")*/

    conf.setAppName("cassandra test")
    val sc = new SparkContext(conf)

    //prepare
    CassandraConnector(conf).withSessionDo { session =>
      session.execute("CREATE KEYSPACE IF NOT EXISTS wqs WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute("CREATE TABLE IF NOT EXISTS wqs.info (log_id bigint PRIMARY KEY, oper_time text, oper_uesr_name text, oper_userid text, source_app text, dest_app text, dst_ip text, request_type text, oper_type text, resource_details text, query_obj_info text, query_obj_type text, email text, src_ip text, src_mac text, oper_content text, oper_result text)")
    }

    //load cassandra connector
    import com.datastax.spark.connector._

    // save data to cassandra
    sc.textFile("/terry/wqs20161110.log")
      .map(x => {
        val f = x.split("\\001")
        WQSLog(f(0).toLong, f(1), f(2), f(3), f(4), f(5), f(6), f(7), f(8), f(9), f(10), f(11), f(12), f(13), f(14), f(15), f(16))
      }).saveToCassandra("wqs", "info", SomeColumns("log_id", "oper_time", "oper_uesr_name", "oper_userid", "source_app", "dest_app", "dst_ip", "request_type", "oper_type", "resource_details", "query_obj_info", "query_obj_type", "email", "src_ip", "src_mac", "oper_content", "oper_result"))

  }
}
