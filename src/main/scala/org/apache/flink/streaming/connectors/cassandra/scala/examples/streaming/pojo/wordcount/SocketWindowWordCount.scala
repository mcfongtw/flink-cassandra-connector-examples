/*
 * PUT COPYRIGHT DISCLAIMER / LICENSE HERE
 */

package org.apache.flink.streaming.connectors.cassandra.scala.examples.streaming.pojo.wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.connectors.cassandra.CassandraSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.cassandra.example.datamodel.WordCountDataModel
import org.apache.flink.streaming.connectors.cassandra.example.datamodel.pojo.WordCount

/**
  * Implements a streaming windowed version of the "WordCount" program.
  *
  * This program connects to a server socket and reads strings from the socket.
  * The easiest way to try this out is to open a text sever (at port 12345)
  * using the ''netcat'' tool via
  * {{{
  * nc -l 12345
  * }}}
  * and run this example with the hostname and the port as arguments..
  */
object SocketWindowWordCount {

  /** Main program method */
  def main(args: Array[String]) : Unit = {

    // the host and the port to connect to
    var hostname: String = "localhost"
    var port: Int = 0

    try {
      val params = ParameterTool.fromArgs(args)
      hostname = if (params.has("hostname")) params.get("hostname") else "localhost"
      port = params.getInt("port")
    } catch {
      case e: Exception => {
        System.err.println("No port specified. Please run 'SocketWindowWordCount " +
          "--hostname <hostname> --port <port>', where hostname (localhost by default) and port " +
          "is the address of the text server")
        System.err.println("To start a simple text server, run 'netcat -l <port>' " +
          "and type the input text into the command line")
        return
      }
    }

    val dataModel = new WordCountDataModel()

    dataModel.setUpEmbeddedCassandra()
    dataModel.setUpDataModel()

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data by connecting to the socket
    val text: DataStream[String] = env.socketTextStream(hostname, port, '\n')

    // parse the data, group it, window it, and aggregate the counts
    val result: DataStream[(String, Long)] = text
      // split up the lines in pairs (2-tuples) containing: (word,1)
      .flatMap(_.toLowerCase.split("\\s"))
      .filter(_.nonEmpty)
      .map((_, 1L))
      // group by the tuple field "0" and sum up tuple field "1"
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)

    CassandraSink.addSink(result)
      .setQuery("INSERT INTO " + WordCount.CQL_KEYSPACE_NAME + "." + WordCount.CQL_TABLE_NAME + "(word, count) " + "values (?, ?);")
      .setHost("127.0.0.1")
      .build()

    result.print().setParallelism(1)

    env.execute("Socket Window WordCount")
  }
}