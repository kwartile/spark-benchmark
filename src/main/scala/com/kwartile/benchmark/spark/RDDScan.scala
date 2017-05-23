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
  * Reads the customers file as defined (http://docs.aws.amazon.com/emr/latest/DeveloperGuide/impala-optimization.html)
  * and performs filter by customer name.
  */

object RDDScan {
  Logger.getLogger("org").setLevel(Level.ERROR);
  Logger.getLogger("akka").setLevel(Level.ERROR);

  def main(args: Array[String]) {
    val parser = parseArguments(args)
    parser.parse(args, InputParams()).map({ params =>
      val sparkConf = new SparkConf().setAppName("Spark-Scan-Performance-Benchmark")
      val sc = new SparkContext(sparkConf)
      runScan(sc, params)
    }).getOrElse {
      println("No Input Parameters, specify input directory")
    }
  }

  def runScan(sc: SparkContext, params: InputParams) {
    println("Running scan ...")
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

    val filterTest = customers.filter(r => r._2 == params.customerName)
    filterTest.take(10).foreach(println)
    println("Showing 10 records")
  }

  def parseArguments(args: Array[String]): OptionParser[InputParams] = {
    new OptionParser[InputParams]("Spark-Scan-Performance-Benchmark") {
      opt[String]('i',"input-path") required() action {
        (x,c) => c.copy(inputPath = x)
      } text "Input Path"
      opt[String]('c',"customer-name") action {
        (x,c) => c.copy(customerName = x)
      } text "Customer Name"
    }
  }

  case class InputParams(inputPath: String = "",
                         customerName: String = "Scarlett STEVENS"
                              )
}
