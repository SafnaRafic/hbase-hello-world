package com.kitmenke.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Delete, Get, Put, Result, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.logging.log4j.{LogManager, Logger}


object MyApp {
  lazy val logger: Logger = LogManager.getLogger(this.getClass)

  implicit def stringToBytes(s: String): Array[Byte] = Bytes.toBytes(s) //To convert string to bytes implicitly


  def main(args: Array[String]): Unit = {
    logger.info("MyApp starting...")
    var connection: Connection = null

    try {
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", "cdh01.hourswith.expert:2181,cdh02.hourswith.expert:2181,cdh03.hourswith.expert:2181")
      connection = ConnectionFactory.createConnection(conf)
      challenge1(connection)
      challenge2(connection)
      challenge3(connection)
      challenge4(connection)
      challenge5(connection)
    } catch {
      case e: Exception => logger.error("Error in main", e)
    } finally {
      if (connection != null) connection.close()
    }
  }


  def printResult(result: Result): Unit = {
    val name = Bytes.toString(result.getValue("f1", "name"))
    val username = Bytes.toString(result.getValue("f1", "username"))
    val sex = Bytes.toString(result.getValue("f1", "sex"))
    val color = Bytes.toString(result.getValue("f1", "favorite_color"))
    val mail = Bytes.toString(result.getValue("f1", "mail"))
    val bday = Bytes.toString(result.getValue("f1", "birthdate"))
    logger.debug(s"name = $name, username = $username, sex = $sex, color = $color, email = $mail, birthday = $bday")
  }

  // Challenge #1: What is user=10000001 email address? Determine this by using a Get that only returns that user's email address, not their complete column list.
  def challenge1(connection: Connection): Unit = {
    val table = connection.getTable(TableName.valueOf("srafic:users"))
    val get = new Get(Bytes.toBytes("10000001"))
    val result = table.get(get)
    val emailAddress: Array[Byte] = result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("mail"))
    val emailAddressAsString = Bytes.toString(emailAddress)
    printResult(result)
    logger.debug(emailAddressAsString)
  }

  //Challenge #2: Write a new user to your table with:
  //Rowkey: 99
  //username: DE-HWE
  //name: The Panther
  //sex: F
  //favorite_color: pink
  def challenge2(connection: Connection): Unit = {
    val table = connection.getTable(TableName.valueOf("srafic:users"))
    val put = new Put(Bytes.toBytes("99"))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("username"), Bytes.toBytes("DE-HWE"));
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes("The Panther"));
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("sex"), Bytes.toBytes("F"));
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("favorite_color"), Bytes.toBytes("pink"));
    table.put(put)
    val get = new Get(Bytes.toBytes("99"))
    val result = table.get(get)
    printResult(result)
  }

  //Challenge #3: How many user IDs exist between 10000001 and 10006001? (Not all user IDs exist, so the answer is not 6000
  def challenge3(connection: Connection): Unit = {
    val table = connection.getTable(TableName.valueOf("srafic:users"))
    val scan = new Scan().withStartRow("10000001").withStopRow("10006001")
    val result = table.getScanner(scan);
    import scala.collection.JavaConverters._
    logger.debug("Number of user between 10000001 and 10006001 : "+ result.iterator().asScala.size)
  }

  //Challenge #4: Delete the user with ID = 99 that we inserted earlier.
  def challenge4(connection: Connection): Unit = {
    val table = connection.getTable(TableName.valueOf("srafic:users"))
    val d = new Delete("99")
    table.delete(d)
    val get = new Get(Bytes.toBytes("99"))
    val result = table.get(get)
//    printResult(result)
  }

  def challenge5(connection: Connection): Unit = {
    val table = connection.getTable(TableName.valueOf("srafic:users"))
    import scala.collection.JavaConverters._
    val gets = List("9005729","500600","30059640","6005263","800182")
      .map(key =>new Get(key).addColumn("f1","mail")).asJava
    val result =table.get(gets)
    result.foreach(printResult)
  }
}
