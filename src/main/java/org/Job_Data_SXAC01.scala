package org


import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Timestamp}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._

/**
  * @Acthor Tao.Lee
  * @date 2022/3/12 21:53
  * @Description说明： oracle到postgers数据库 流处理
  */

object Job_Data_SXAC01 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.addSource(new OracleSource)
      .name("OracleSource")
      .addSink(new MySqlSink)
      .name("MySqlSink")
      .setParallelism(1)

    env.execute("Data From Oracle To Postgre From LT&EHL")
  }

  def NowDate(): String = {
    val now = new Date()
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val date = dateFormat.format(now)
    date
  }

  def NowTime(): Long ={
    val now = new Date()
    val a = now.getTime
    a.toLong
  }
}

case class ac01(
                 aac001 : String ,
                 aac002 : String ,
                 aac003 : String ,
                 aac004 : String ,
                 aac005 : String ,
                 aac006 : Float ,
                 aae005 : String ,
                 aae006 : String ,
                 aae007 : String ,
                 aae013 : String ,
                 aae159 : String ,
                 aac017 : String ,
                 aac060 : String ,
                 aae014 : String ,
                 aae004 : String ,
                 aac084 : String ,
                 dabh : String,
                 aac010 : String ,
                 aac085 : String,
                 dxp_share_batch_id : String  ,
                 dxp_share_timestamp : Timestamp
               )


class OracleSource extends RichSourceFunction[ac01] {

  var conn: Connection = _
  var selectStmt: PreparedStatement = _
  var isRunning: Boolean = true

  override def open(parameters: Configuration): Unit = {
    // 加载驱动
    Class.forName("oracle.jdbc.driver.OracleDriver")
    // 数据库连接
    conn = DriverManager.getConnection("jdbc:oracle:thin:@19.167.4.228:1521/sxsjzt1", "yhlcx", "qyyyhlcx1221")
    selectStmt = conn.prepareStatement("select * from cxjmyl.ac01 where dxp_share_timestamp > SYSDATE-1")
  }

  override def run(sourceContext: SourceFunction.SourceContext[ac01]): Unit = {

    while (isRunning) {
      var resultSet: ResultSet = selectStmt.executeQuery()
      while (resultSet.next()) {
        var aac001: String = resultSet.getString("aac001")
        var aac002: String = resultSet.getString("aac002")
        var aac003: String = resultSet.getString("aac003")
        var aac004: String = resultSet.getString("aac004")
        var aac005: String = resultSet.getString("aac005")
        var aac006: Float = resultSet.getFloat("aac006")
        var aae005: String = resultSet.getString("aae005")
        var aae006: String = resultSet.getString("aae006")
        var aae007: String = resultSet.getString("aae007")
        var aae013: String = resultSet.getString("aae013")
        var aae159: String = resultSet.getString("aae159")
        var aac017: String = resultSet.getString("aac017")
        var aac060: String = resultSet.getString("aac060")
        var aae014: String = resultSet.getString("aae014")
        var aae004: String = resultSet.getString("aae004")
        var aac084: String = resultSet.getString("aac084")
        var dabh: String = resultSet.getString("dabh")
        var aac010: String = resultSet.getString("aac010")
        var aac085: String = resultSet.getString("aac085")
        var dxp_share_batch_id: String = resultSet.getString("dxp_share_batch_id")
        var dxp_share_timestamp: Timestamp = resultSet.getTimestamp("dxp_share_timestamp")
        sourceContext.collect(ac01(aac001, aac002,aac003, aac004,aac005,aac006,aae005,
          aae006,aae007,aae013,aae159,aac017,aac060,aae014,aae004,aac084,dabh,aac010,
          aac085,dxp_share_batch_id,dxp_share_timestamp))
      }
      println(Job_Data_SXAC01.NowDate() + "暂停60s")
      Thread.sleep(60000)
    }
  }

  override def close(): Unit = {
    selectStmt.close()
    conn.close()
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}


class MySqlSink extends RichSinkFunction[ac01] {

  var conn: Connection = _
  var selectStmt: PreparedStatement = _
  var insertStmt: PreparedStatement = _
  //var updateStmt: PreparedStatement = _

  override def close(): Unit = {
    insertStmt.close()
    //updateStmt.close()
    conn.close()
  }

  override def open(parameters: Configuration): Unit = {

    Class.forName("org.postgresql.Driver")
    conn = DriverManager.getConnection("jdbc:postgresql://19.167.4.236:5432/sxsbjdb"
      , "gpadmin"
      , "gpadmin")
    insertStmt = conn.prepareStatement("INSERT INTO cxjmyl.ac01 " +
      "(aac001, aac002,aac003, aac004,aac005,aac006,aae005," +
      "aae006,aae007,aae013,aae159,aac017,aac060,aae014,aae004," +
      "aac084,dabh,aac010,aac085,dxp_share_batch_id,dxp_share_timestamp) " +
      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
    //updateStmt = conn.prepareStatement("UPDATE  cxjmyl.ltac01 SET aac002 =? , dxp_share_timestamp = ?  WHERE aac001 = ?")
    selectStmt = conn.prepareStatement("select aac001 from cxjmyl.ac01 where aac001=? ")
  }

  override def invoke(value: ac01): Unit = {

    // 执行更新语句
    //updateStmt.setString(1, value.aac002)
    //updateStmt.setTimestamp(2, new Timestamp(value.dxp_share_timestamp.getTime))
    //updateStmt.setString(3, value.aac001)
    //updateStmt.execute()
    //如果update没有更新 即 没有查询到数据 即 没有该id 那么执行插入
    //if (updateStmt.getUpdateCount == 0) {
      //println(Job_Data_SXAC01.NowDate() + "------------------插入数据>>" + value)
    //先查询有没有数据再进行插入
    selectStmt.setString(1, value.aac001)
    selectStmt.execute()
    if(selectStmt.getResultSet == null) {
      insertStmt.setString(1, value.aac001)
      insertStmt.setString(2, value.aac002)
      insertStmt.setString(3, value.aac003)
      insertStmt.setString(4, value.aac004)
      insertStmt.setString(5, value.aac005)
      insertStmt.setFloat(6, value.aac006)
      insertStmt.setString(7, value.aae005)
      insertStmt.setString(8, value.aae006)
      insertStmt.setString(9, value.aae007)
      insertStmt.setString(10, value.aae013)
      insertStmt.setString(11, value.aae159)
      insertStmt.setString(12, value.aac017)
      insertStmt.setString(13, value.aac060)
      insertStmt.setString(14, value.aae014)
      insertStmt.setString(15, value.aae004)
      insertStmt.setString(16, value.aac084)
      insertStmt.setString(17, value.dabh)
      insertStmt.setString(18, value.aac010)
      insertStmt.setString(19, value.aac085)
      insertStmt.setString(20, value.dxp_share_batch_id)
      insertStmt.setTimestamp(21, new Timestamp(Job_Data_SXAC01.NowTime())) //value.dxp_share_timestamp.getTime))
      insertStmt.execute()
      println(Job_Data_SXAC01.NowDate() + "------------------已插入这条数据>>" +value.aac001+" "+value.dxp_share_batch_id)
    }  else {
      println(Job_Data_SXAC01.NowDate() + "------------------已存在，不插入>>" +value.aac001+" "+value.dxp_share_batch_id)
    }

  }

}
