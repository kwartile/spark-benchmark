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

package com.kwartile.benchmark.spark

import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.util.Calendar

/**
  * Reads the books and transactions files as defined (http://docs.aws.amazon.com/emr/latest/DeveloperGuide/impala-optimization.html)
  * filters transactions by year, join transactions and books on book category, counts the total amount by category, and
  * sorts the results on the total amount.
  */

object RDDTwoWayJoin {
  Logger.getLogger("org").setLevel(Level.ERROR);
  Logger.getLogger("akka").setLevel(Level.ERROR);

  def main(args: Array[String]) {
    val parser = parseArguments(args)

    /**
      *
      */
    parser.parse(args, InputParams()).map({ params =>
      val sparkConf = new SparkConf().setAppName("Spark-TwoWayJoin-Performance-Benchmark")
      val sc = new SparkContext(sparkConf)
      runTwoWayJoin(sc, params)
      sc.stop()
    }).getOrElse {
      println("No Input Parameters, specify input directory")
    }
  }

  def runTwoWayJoin(sc: SparkContext, params: InputParams): Unit = {
    println("Running 2-way join ...")

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

    val dtFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val transactions = sc.textFile(params.inputPath + "/transactions/transactions")
      .map(r => {
        val columns = r.split('|')
        val tId = columns(0)
        val custId = columns(1)
        val bookId = columns(2)
        val qty = columns(3).toInt
        val tDate = dtFormat.parse(columns(4))

        (tId, custId, bookId, qty, tDate)
      })

    // get top grossing book category
    val transactionsFilteredByDate = transactions.filter(x => {
      val cal = Calendar.getInstance()
      cal.setTime(x._5)
      val year = cal.get(Calendar.YEAR)
      year >= 2008 && year <= 2010
    }).map(x => {
      val bookId = x._3
      val qty = x._4
      (bookId, qty)
    })

    val topGrossingBookCategory = books.map(x => {
      val bookId = x._1
      val category = x._3
      val price = x._6
      (bookId, (category, price))
    }).leftOuterJoin(transactionsFilteredByDate)
      .map(x => {
        val category = x._2._1._1
        val qty = x._2._2.getOrElse(0)
        val price = x._2._1._2
        val tAmount = BigDecimal(price * qty).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
        (category, tAmount)
      })
      .reduceByKey((a, b) => a + b).takeOrdered(10)(Ordering[Double].reverse.on[(String, Double)](_._2))

    topGrossingBookCategory.foreach(println)
    println("Showing top 10 grossing categories between 2008 and 2010")
  }

  def parseArguments(args: Array[String]): OptionParser[InputParams] = {
    new OptionParser[InputParams]("Spark-TwoWayJoin-Performance-Benchmark") {
      opt[String]('i',"input-path") required() action {
        (x,c) => c.copy(inputPath = x)
      } text "Input Path"
      opt[Int]('p',"shuffle-partition-count")  action {
        (x,c) => c.copy(shufflePartitionCount = x)
      } text "Shuffle Partition Count"
    }
  }

  case class InputParams(inputPath: String = "",
                         shufflePartitionCount: Integer = 0  // not used
                         )

}
