package com.study.spark.streaming.streaming

import com.study.spark.streaming.streaming.Util.StreamingExamples
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NetworkWordCountWithCheckPoint {
  /**
   * Usage: NetworkWordCount <master> <hostname> <port>
   * <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.
   *
   * To run this on your local machine, you need to first run a Netcat server
   *    `$ nc -lk 9990`
   *
   * and then run the example
   *    `$ bin/run-example com.study.spark.streaming.streaming.NetworkWordCountSimple localhost 9990`
   * @param args
   */
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: NetworkWordCount <master> <hostname> <port> <duration> <checkpoint directory>")
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    val ssc = StreamingContext.getOrCreate(args(4), () => createContext(args))


    ssc.start()

    ssc.awaitTermination()
  }

  def createContext(args: Array[String]) = {

    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setMaster(args(0)).setAppName("NetworkWordCount")
    sparkConf.set("spark.streaming.receiver.writeAheadLog.enable", "true")

    val ssc = new StreamingContext(sparkConf, Seconds(args(3).toInt))

    // Create a socket stream(ReceiverInputDStream) on target ip:port
    val lines = ssc.socketTextStream(args(1), args(2).toInt, StorageLevel.MEMORY_AND_DISK_SER)

    // Split words by space to form DStream[String]
    val words = lines.flatMap(_.split(" "))

    // count the words to form DStream[(String, Int)]
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    wordCounts.print()

    ssc.checkpoint(args(4))

    ssc
  }
}