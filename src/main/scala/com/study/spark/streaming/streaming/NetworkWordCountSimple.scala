package com.study.spark.streaming.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

import com.study.spark.streaming.streaming.Util.StreamingExamples

object NetworkWordCountSimple {
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
      System.err.println("Usage: NetworkWordCount <master> <hostname> <port>")
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setMaster(args(0)).setAppName("NetworkWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(args(3).toInt))

    // Create a socket stream(ReceiverInputDStream) on target ip:port
    val lines = ssc.socketTextStream(args(1), args(2).toInt, StorageLevel.MEMORY_ONLY)

    // Split words by space to form DStream[String]
    val words = lines.flatMap(_.split(" "))

    // count the words to form DStream[(String, Int)]
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    // print the word count in the batch
    wordCounts.print()

    ssc.start()

    ssc.awaitTermination()
  }
}