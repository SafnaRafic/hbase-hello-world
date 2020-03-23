package com.kitmenke.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger

object MyApp {
  lazy val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("MyApp starting...")
    var connection: Connection = null
    try {
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", "hbase-docker")
      connection = ConnectionFactory.createConnection(conf)
      val table = connection.getTable(TableName.valueOf("table-name"))
      val get = new Get(Bytes.toBytes("row-key"))
      val result = table.get(get)
      logger.debug(result)
    } catch {
      case e: Exception => logger.error("Error in main", e)
    } finally {
      if (connection != null) connection.close()
    }
  }
}
