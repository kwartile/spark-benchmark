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

/**
  * Reads the books, transactions, and customers files as defined (http://docs.aws.amazon.com/emr/latest/DeveloperGuide/impala-optimization.html)
  * filters the customers by state, joins transactions and customers on state, joins the results with books, counts the total amount by category, and
  * sorts the results on the total amount.
  */

object RDDThreeWayJoin {
  Logger.getLogger("org").setLevel(Level.ERROR);
  Logger.getLogger("akka").setLevel(Level.ERROR);

  def main(args: Array[String]) {
    val parser = parseArguments(args)

    /**
      *
      */
    parser.parse(args, InputParams()).map({ params =>
      val sparkConf = new SparkConf().setAppName("Spark-ThreeWayJoin-Performance-Benchmark")
      val sc = new SparkContext(sparkConf)
      runThreeWayJoin(sc, params)
      sc.stop()
    }).getOrElse {
      println("No Input Parameters, specify input directory")
    }
  }

  def runThreeWayJoin(sc: SparkContext, params: InputParams): Unit = {

    val customers = sc.textFile(params.inputPath + "/customers/customers")
      .map(r => {
        val columns = r.split('|')
        val custId = columns(0)
        val name = columns(1)
        val dob = columns(2)
        val gender = columns(3)
        val state = columns(4)
        val email = columns(5)
        val acctNum = columns(6)

        (custId, name, dob, gender, state, email, acctNum)
      })

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

    // top grossing book cetegory by region
    val customerFilteredByStates = customers.filter((x => {
      val state = x._5
      state == "WA" || state == "CA" || state == "NY"
    })).map(x => {
      val custId = x._1
      val state = x._5
      (custId, state)
    })

    val transactionsFilteredByState = transactions.map(x => {
      val custId = x._2
      val bookId = x._3
      val qty = x._4
      (custId, (bookId, qty))
    }).join(customerFilteredByStates)
      .map(x => {
        val custId = x._1
        val bookId = x._2._1._1
        val qty =  x._2._1._2
        val state = x._2._2
        (bookId, qty)
      })

    val topGrossingBookCategoryByState = books.map(x => {
      val bookId = x._1
      val category = x._3
      val price = x._6
      (bookId, (category, price))
    }).leftOuterJoin(transactionsFilteredByState)
      .map(x => {
        val bookId = x._1
        val category = x._2._1._1
        val price = x._2._1._2
        val qty = x._2._2.getOrElse(0)
        val tAmount = BigDecimal(price * qty).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
        (category, tAmount)
      })
      .reduceByKey((a, b) => a + b).takeOrdered(10)(Ordering[Double].reverse.on[(String, Double)](_._2))

    println("Running 3-way join...")
    topGrossingBookCategoryByState.foreach(println)
    println("Top 10 grossing categories and in the states WA, CA, and NY")
  }

  def parseArguments(args: Array[String]): OptionParser[InputParams] = {
    new OptionParser[InputParams]("Spark-ThreeWayJoin-Performance-Benchmark") {
      opt[String]('i',"input-path") required() action {
        (x,c) => c.copy(inputPath = x)
      } text "Input Path"
      opt[Int]('p',"shuffle-partition-count")  action {
        (x,c) => c.copy(shufflePartitionCount = x)
      } text "Shuffle Partition Count"
    }
  }

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }

  case class InputParams(inputPath: String = "",
                         shufflePartitionCount: Integer = 0
                         )

}
