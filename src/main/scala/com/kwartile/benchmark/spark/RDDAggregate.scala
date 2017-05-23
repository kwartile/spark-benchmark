/**
  * Copyright (c) 2017 Kwartile, Inc., http://www.kwartile.com
  * Permission is hereby granted, free of charge, to any person obtaining a copy
  * of this software and associated documentation files (the "Software"), to deal
  * in the Software without restriction, including without limitation the rights
  * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  * copies of the Software, and to permit persons to whom the Software is
  * furnished to do so, subject to the following conditions:
  *
  * The above copyright notice and this permission notice shall be included in all
  * copies or substantial portions of the Software.
  *
  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  * SOFTWARE.
  */

/**
  * Reads the books file as defined (http://docs.aws.amazon.com/emr/latest/DeveloperGuide/impala-optimization.html)
  * and counts number of books per category
  */

package com.kwartile.benchmark.spark

import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser
import org.apache.log4j.Logger
import org.apache.log4j.Level


object RDDAggregate {
  Logger.getLogger("org").setLevel(Level.ERROR);
  Logger.getLogger("akka").setLevel(Level.ERROR);

  def main(args: Array[String]) {
    val parser = parseArguments(args)
    parser.parse(args, InputParams()).map({ params =>
      val sparkConf = new SparkConf().setAppName("Spark-Aggregate-Performance-Benchmark")
      val sc = new SparkContext(sparkConf)
      runAggregate(sc, params)
      sc.stop()
    }).getOrElse {
      println("No Input Parameters, specify input directory")
    }
  }

  def runAggregate(sc: SparkContext, params: InputParams): Unit = {
    println("Running aggregations ...")
    val books = sc.textFile(params.inputPath + "/books/books")
      .map(r => {
        val columns = r.split('|')
        val bookId = columns(0)
        val isbn = columns(1)
        val category = columns(2)
        val pubDate = columns(3)
        val publisher = columns(4)
        val price = columns(5).toDouble

        (bookId, isbn, category, pubDate, publisher, price)
      })

    val bookCountByCategory = books.map(x => (x._3, 1))
      .reduceByKey((a, b) => a + b).takeOrdered(10)(Ordering[Int].reverse.on[(String, Int)](_._2))

    bookCountByCategory.foreach(println)
    println("Showing top 10 categories with most number of books.")
  }

  def parseArguments(args: Array[String]): OptionParser[InputParams] = {
    new OptionParser[InputParams]("Spark-Aggregate-Performance-Benchmark") {
      opt[String]('i',"input-path") required() action {
        (x,c) => c.copy(inputPath = x)
      } text "Input Path"
      opt[Int]('p',"shuffle-partition-count") action {
        (x,c) => c.copy(shufflePartitionCount = x)
      } text "Partition Count"
    }
  }

  case class InputParams(inputPath: String = "",
                               shufflePartitionCount: Integer = 0
                              )
}
