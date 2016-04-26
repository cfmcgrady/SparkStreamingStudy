import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.SynchronizedQueue

object QueueStream2 {
  def formatTs(ts: Long) = {
    val sdf = new SimpleDateFormat("YYYY/MM/dd HH:mm:ss")
    val date = new Date(ts)
    sdf.format(date)
  }

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("QueueStream")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // Create the queue through which RDDs can be pushed to
    // a QueueInputDStream
    val rddQueue = new SynchronizedQueue[RDD[Int]]()

    // Create the QueueInputDStream and use it do some processing
    val inputStream = ssc.queueStream(rddQueue)

    val d1 = inputStream.foreachRDD((rdd, time) => {
      rdd.setName("rddname" + formatTs(time.milliseconds))
      val cacheRdd = rdd.cache()
      val count = cacheRdd.count
      val r1 = if (count > 100) cacheRdd.sample(false, 0.1, System.currentTimeMillis) else cacheRdd

      r1.mapPartitions(it => {
        Thread.sleep(60000)
        it.map(_ + 1)
      }).take(2).foreach(println)

    })



    ssc.start()

    // Create and push some RDDs into
    while(true) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 1000, 10)
      Thread.sleep(3000)
    }
    ssc.awaitTermination()
  }
}
