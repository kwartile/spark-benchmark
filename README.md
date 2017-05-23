## Spark Benchmark

#### Overview
Spark Benchmark suite helps you evaluate Spark cluster configuration.  This benchmark can also be used to compare the speed, throughput, and resource usage of Spark jobs with other big data frameworks such as Impala and Hive. It contains a set of Spark RDD based operations that performs map, filter, reduceByKey, and join operations.

#### Data
The benchmark uses the dataset used for Impala performance measurement (http://docs.aws.amazon.com/emr/latest/DeveloperGuide/query-impala-generate-data.html).  The dataset consists of three different files:
* Books
* Customers
* Transactions

```
> head books
0|5-54687-602-6|FOREIGN-LANGUAGE-STUDY|1989-09-29|Saraiva|83.99
1|7-20527-497-2|PHILOSOPHY|1999-09-14|Kyowon|40.99
2|8-98211-350-2|JUVENILE-NONFICTION|1975-01-11|Wolters Kluwer|173.99
3|6-52228-529-3|MATHEMATICS|2010-06-26|Bungeishunju|24.99
4|8-98702-825-4|HUMOR|1990-07-15|China Publishing Group Corporate|64.99
5|3-11023-371-2|LITERARY-CRITICISM|1971-06-04|AST|137.99
```

```
> head customers
Customers
0|Sophia PERKINS|1975-11-18|F|OK|sophia.perkins.1975@gmail.com|963-341-4876
1|Brianna MURRAY|2001-11-02|F|MT|brianna.murray.2001@gmail.com|260-164-6277
2|James SCOTT|1997-09-17|M|UT|james.scott.1997@gmail.com|920-899-8587
3|Samuel GREEN|2013-05-22|F|CO|samuel.green.2013@live.com|263-707-8321
4|Logan COLEMAN|1997-12-10|F|NE|logan.coleman.1997@hotmail.com|333-318-5685
5|Matthew BENNETT|1975-12-26|M|CO|matthew.bennett.1975@outlook.com|717-808-3733
6|Jace SPENCER|2013-10-30|M|KS|jace.spencer.2013@live.com|448-105-3939
```

```
> head transactions
0|29948726|124004825|21|2000-10-03 12:08:37
1|76896577|10225228|17|2001-04-23 15:21:18
2|77394742|62037151|23|2008-02-22 11:52:36
3|23558280|21960491|29|2000-06-22 10:14:48
4|5742930|73207419|15|2004-11-26 00:46:53
5|101531051|122609274|13|2008-01-14 05:26:46
```
#### Benchmark
The benchmarks contains four tests:
RDDScan: RDDScan reads the customer file and performs a filter operation.  It is equivalent to a select-where statement in SQL.
RDDAggregate: RDDAgregate operation scans the books file and perform reduceByKey to aggregate count of books by category.  It then sorts the results based on the book count.
RDDTwoWayJoin: This operation performs a join between books and transactions between 2008 and 2010, aggregates the results on book category, and returns sorted results based on the total transaction amount.
RDDThreeWayJoin: This operation is similar to the above except we perform addition join with the customer table and filter the results on three states
Supported Platform
The pom file currently include support for Spark 1.6 on CDH 5.8.  But this can be easily modified to run on Spark 2.x and other version of Cloudera, HortonWorks, and Apache distribution.

#### Build & Run
You use standard maven command to build.  You use the following command to run the job:
```
spark-submit  --class com.kwartile.benchmark.spark.RDDAggregate --master yarn --executor-memory <mem> --executor-cores <num> --num-executors <num>  --conf spark.yarn.executor.memoryOverhead=<mem_in_mb> perf-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar --input-path <hdfs location>
```
#### Hive and Impala Query
You can use the following equivalent Hive/Impala query to compare the performance.

```
# Scan Query
SELECT COUNT(*)
FROM customers256gb
WHERE name = 'Harrison SMITH';
```

```
# Aggregation
SELECT category, count(*) cnt
FROM books256gb
GROUP BY category
ORDER BY cnt DESC LIMIT 10;
```

```
# Two Way Join
SELECT tmp.book_category, ROUND(tmp.revenue, 2) AS revenue
FROM (
SELECT books256gb.category AS book_category, SUM(books256gb.price * transactions256gb.quantity) AS revenue
FROM books256gb JOIN transactions256gb ON (
transactions256gb.book_id = books256gb.id
AND YEAR(transactions256gb.transaction_date) BETWEEN 2008 AND 2010
)
GROUP BY books256gb.category
) tmp
ORDER BY revenue DESC LIMIT 10;
```

```
# Three Way Join
SELECT tmp.book_category, ROUND(tmp.revenue, 2) AS revenue
FROM (
  SELECT books256gb.category AS book_category, SUM(books256gb.price * transactions256gb.quantity) AS revenue
  FROM books256gb
  JOIN transactions256gb ON (
    transactions256gb.book_id = books256gb.id
  )
  JOIN customers256gb ON (
    transactions256gb.customer_id = customers256gb.id
    AND customers256gb.state IN ('WA', 'CA', 'NY')
  )
  GROUP BY books256gb.category
) tmp
ORDER BY revenue DESC LIMIT 10;
```

