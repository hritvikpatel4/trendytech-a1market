package com.scala.assignment

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.io.AnsiColor._

object A1Market {
  def main(args: Array[String]): Unit = {
    if (args.length != 6) {
      throw new IllegalArgumentException(
        "6 Arguments required: <inputPathOrders> <inputPathCustomers> <parquetPathOrders> <parquetPathCustomers> <outputPathOrders> <outputPathCustomers>"
      )
    }

    // Read command line args for paths
    val inputPathOrders = args(0)
    val inputPathCustomers = args(1)
    val parquetPathOrders = args(2)
    val parquetPathCustomers = args(3)
    val outputPathOrders = args(4)
    val outputPathCustomers = args(5)

    val DFSTRING = s"${RED}Using Dataframe${RESET}"
    val SQLSTRING = s"${RED}Using SparkSQL${RESET}"
    val DIVIDER = s"""${BLACK_B}${WHITE}----------------------------------------------------------------------
                      |----------------------------------------------------------------------------------------
                      |-----------------${RESET}""".stripMargin.replaceAll("\n", "")

// --------------------------------------------------------------------------------------------------------------------------------------------

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("A1Market Hritvik")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // Print out the passed args to user
    println(s"${BLACK_B}${CYAN}Orders input path: ${inputPathOrders}${RESET}")
    println(s"${BLACK_B}${CYAN}Customers input path: ${inputPathCustomers}${RESET}")
    println(s"${BLACK_B}${CYAN}Orders parquet path: ${parquetPathOrders}${RESET}")
    println(s"${BLACK_B}${CYAN}Customers parquet path: ${parquetPathCustomers}${RESET}")
    println(s"${BLACK_B}${CYAN}Orders output path: ${outputPathOrders}${RESET}")
    println(s"${BLACK_B}${CYAN}Customers output path: ${outputPathCustomers}${RESET}")

// --------------------------------------------------------------------------------------------------------------------------------------------

    println(DIVIDER)

    println(s"${WHITE}ENHANCEMENT:\n\tReading CSV data from HDFS and writing it as PARQUET in HDFS${RESET}")

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val parquetOrders = new Path(parquetPathOrders)
    val parquetCustomers = new Path(parquetPathCustomers)

    if (fs.exists(parquetOrders) && fs.exists(parquetCustomers) && fs.isDirectory(parquetOrders) && fs.isDirectory(parquetCustomers)) {
      fs.delete(parquetOrders, true)
      fs.delete(parquetCustomers, true)
    }

    println(s"${RED}\tDone...${RESET}")

    println(DIVIDER)

// --------------------------------------------------------------------------------------------------------------------------------------------

    // Read Orders dataset from HDFS and store output in Parquet format on HDFS
    val tempOrders = spark
      .read
      .option("inferSchema", "true")
      .csv(inputPathOrders)
      .withColumnRenamed("_c0", "order_id")
      .withColumnRenamed("_c1", "order_date")
      .withColumnRenamed("_c2", "order_customer_id")
      .withColumnRenamed("_c3", "order_status")

    tempOrders
      .write
      .format("parquet")
      .mode("overwrite")
      .save(parquetPathOrders)

    // Read Customers dataset from HDFS and store output in Parquet format on HDFS
    val tempCustomers = spark
      .read
      .option("inferSchema", "true")
      .csv(inputPathCustomers)
      .withColumnRenamed("_c0", "customer_id")
      .withColumnRenamed("_c1", "customer_fname")
      .withColumnRenamed("_c2", "customer_lname")
      .withColumnRenamed("_c3", "customer_email")
      .withColumnRenamed("_c4", "customer_password")
      .withColumnRenamed("_c5", "customer_street")
      .withColumnRenamed("_c6", "customer_city")
      .withColumnRenamed("_c7", "customer_state")
      .withColumnRenamed("_c8", "customer_zipcode")

    tempCustomers
      .write
      .format("parquet")
      .mode("overwrite")
      .save(parquetPathCustomers)

// --------------------------------------------------------------------------------------------------------------------------------------------

    // Read Orders dataset from HDFS
    val ordersDF = spark
      .read
      .option("inferSchema", "true")
      .parquet(parquetPathOrders)

//    val ordersDF = spark
//      .read
//      .option("inferSchema", "true")
//      .csv(inputPathOrders)
//      .withColumnRenamed("_c0", "order_id")
//      .withColumnRenamed("_c1", "order_date")
//      .withColumnRenamed("_c2", "order_customer_id")
//      .withColumnRenamed("_c3", "order_status")

    ordersDF.show()
//    ordersDF.printSchema()

    ordersDF.createOrReplaceTempView("orders")

    println(s"${BLACK_B}${CYAN}Number of rows in orders = ${ordersDF.count()}${RESET}")

// --------------------------------------------------------------------------------------------------------------------------------------------

    // Read Customers dataset from HDFS
    val customersDF = spark
      .read
      .option("inferSchema", "true")
      .parquet(parquetPathCustomers)
      .withColumn("customer_email", lit("*******"))       // customer_email is already masked, but still masking for uniformity
      .withColumn("customer_password", lit("*******"))    // customer_password is already masked, but still masking for uniformity
      .withColumn("customer_street", lit("*******"))
      .withColumn("customer_zipcode", lit("*******"))

//    val customersDF = spark
//      .read
//      .option("inferSchema", "true")
//      .csv(inputPathCustomers)
//      .withColumnRenamed("_c0", "customer_id")
//      .withColumnRenamed("_c1", "customer_fname")
//      .withColumnRenamed("_c2", "customer_lname")
//      .withColumnRenamed("_c3", "customer_email")
//      .withColumnRenamed("_c4", "customer_password")
//      .withColumnRenamed("_c5", "customer_street")
//      .withColumnRenamed("_c6", "customer_city")
//      .withColumnRenamed("_c7", "customer_state")
//      .withColumnRenamed("_c8", "customer_zipcode")
//      .withColumn("customer_email", lit("*******"))       // customer_email is already masked, but still masking for uniformity
//      .withColumn("customer_password", lit("*******"))    // customer_password is already masked, but still masking for uniformity
//      .withColumn("customer_street", lit("*******"))
//      .withColumn("customer_zipcode", lit("*******"))

    customersDF.show()
//    customersDF.printSchema()

    customersDF.createOrReplaceTempView("customers")

    println(s"${BLACK_B}${CYAN}Number of rows in customers = ${customersDF.count()}${RESET}")

// --------------------------------------------------------------------------------------------------------------------------------------------

    // Inner Join for orders and customers table
    val innerjoinedDF = ordersDF.join(
      customersDF,
      ordersDF("order_customer_id") === customersDF("customer_id"),
      "inner"
    )

    innerjoinedDF.show()
//    innerjoinedDF.printSchema()

    innerjoinedDF.createOrReplaceTempView("joineddata")

//    println(s"${BLACK_B}${CYAN}Number of rows in innerjoined/joineddata = ${innerjoinedDF.count()}${RESET}")

    val leftjoinedDF = customersDF.join(
      ordersDF,
      customersDF("customer_id") === ordersDF("order_customer_id"),
      "left"
    )

//    leftjoinedDF.show()
//    leftjoinedDF.printSchema()

//    println(s"${BLACK_B}${CYAN}Number of rows in leftjoined = ${leftjoinedDF.count()}${RESET}")

// --------------------------------------------------------------------------------------------------------------------------------------------

    // Read input from user for customer name
    print(s"${BLACK_B}${CYAN}Enter Customer Name: ")
    val customer_name = scala.io.StdIn.readLine().split(" ")
    println(s"${RESET}")
    val fname = customer_name(0).trim
    val lname = customer_name(1).trim

    println(DIVIDER)

// --------------------------------------------------------------------------------------------------------------------------------------------

// 1. Retrieve all records for particular customer

    println(s"${RED}TASK 1: Retrieve all records for particular customer = ${fname} ${lname}${RESET}")

    println(DFSTRING)

    val result1DF = customersDF
      .where(s"customer_fname = '${fname}'")
      .where(s"customer_lname = '${lname}'")

    result1DF.show()
    println(s"${BLACK_B}${CYAN}Number of rows in result1DF = ${result1DF.count()}${RESET}")

    println(SQLSTRING)

    val result1DF_SQL = spark.sql(
      s"SELECT * FROM customers WHERE customer_fname = '${fname}' AND customer_lname = '${lname}'"
    )

    result1DF_SQL.show()
    println(s"${BLACK_B}${CYAN}Number of rows in result1DF_SQL = ${result1DF_SQL.count()}${RESET}")

    println(DIVIDER)

// --------------------------------------------------------------------------------------------------------------------------------------------

// 2. List count of orders based on status and month

    println(s"${RED}TASK 2: List count of orders based on status and month${RESET}")

    println(DFSTRING)

    val result2DF = ordersDF
      .groupBy(
        ordersDF("order_status"),
        month(col("order_date")).as("order_month")
      )
      .count()

    result2DF.show()
    println(s"${BLACK_B}${CYAN}Number of rows in result2DF = ${result2DF.count()}${RESET}")

    println(SQLSTRING)

    val result2DF_SQL = spark.sql(
      "SELECT order_status, month(order_date) AS order_month, COUNT(*) AS count FROM orders GROUP BY order_status, order_month"
    )

    result2DF_SQL.show()
    println(s"${BLACK_B}${CYAN}Number of rows in result2DF_SQL = ${result2DF_SQL.count()}${RESET}")

    println(DIVIDER)

// --------------------------------------------------------------------------------------------------------------------------------------------

// 3. List count of orders based on status and month for particular customer

    println(s"${RED}TASK 3: List count of orders based on status and month for particular customer = ${fname} ${lname}${RESET}")

    println(DFSTRING)

    val query_customerDF = innerjoinedDF
      .where(s"customer_fname = '${fname}'")
      .where(s"customer_lname = '${lname}'")

    val result3DF = query_customerDF
      .groupBy(
        query_customerDF("order_status"),
        month(col("order_date")).as("order_month")
      )
      .count()

    result3DF.show()
    println(s"${BLACK_B}${CYAN}Number of rows in result3DF = ${result3DF.count()}${RESET}")

    println(SQLSTRING)

    val result3DF_SQL = spark.sql(
      s"SELECT order_status, month(order_date) AS order_month, COUNT(*) AS count FROM joineddata WHERE customer_fname = '${fname}' AND customer_lname = '${lname}' GROUP BY order_status, order_month"
    )

    result3DF_SQL.show()
    println(s"${BLACK_B}${CYAN}Number of rows in result3DF_SQL = ${result3DF_SQL.count()}${RESET}")

    println(DIVIDER)

// --------------------------------------------------------------------------------------------------------------------------------------------

// 4. List count of orders based on customer and status

    println(s"${RED}TASK 4. List count of orders based on customer and status${RESET}")

    println(DFSTRING)

    val result4DF = innerjoinedDF
      .groupBy(
        "customer_id",
        "order_status"
      )
      .count()

    result4DF.show()
    println(s"${BLACK_B}${CYAN}Number of rows in result4DF = ${result4DF.count()}${RESET}")

    println(SQLSTRING)

    val result4DF_SQL = spark.sql(
      "SELECT customer_id, order_status, COUNT(*) AS count FROM joineddata GROUP BY customer_id, order_status"
    )

    result4DF_SQL.show()
    println(s"${BLACK_B}${CYAN}Number of rows in result4DF_SQL = ${result4DF_SQL.count()}${RESET}")

    println(DIVIDER)

// --------------------------------------------------------------------------------------------------------------------------------------------

// 5. Find the customers who have placed orders

    println(s"${RED}TASK 5. Find the customers who have placed orders${RESET}")

    println(DFSTRING)

    val result5DF = leftjoinedDF
      .select("customer_id", "customer_fname", "customer_lname")
      .where(ordersDF("order_customer_id").isNotNull)

    result5DF.show()
    println(s"${BLACK_B}${CYAN}Number of rows in result5DF = ${result5DF.count()}${RESET}")

    println(SQLSTRING)

    val result5DF_SQL = spark.sql(
      "SELECT customer_id, customer_fname, customer_lname FROM customers LEFT JOIN orders ON customer_id = order_customer_id WHERE order_customer_id IS NOT NULL"
    )

    result5DF_SQL.show()
    println(s"${BLACK_B}${CYAN}Number of rows in result5DF_SQL = ${result5DF_SQL.count()}${RESET}")

    println(DIVIDER)

// --------------------------------------------------------------------------------------------------------------------------------------------

// 6. Find the customers who have not placed orders yet

    println(s"${RED}TASK 6. Find the customers who have not placed orders yet${RESET}")

    println(DFSTRING)

    val result6DF = leftjoinedDF
      .select("customer_id", "customer_fname", "customer_lname")
      .where(ordersDF("order_customer_id").isNull)

    result6DF.show()
    println(s"${BLACK_B}${CYAN}Number of rows in result6DF = ${result6DF.count()}${RESET}")

    println(SQLSTRING)

    val result6DF_SQL = spark.sql(
      "SELECT customer_id, customer_fname, customer_lname FROM customers LEFT JOIN orders ON customer_id = order_customer_id WHERE order_customer_id IS NULL"
    )

    result6DF_SQL.show()
    println(s"${BLACK_B}${CYAN}Number of rows in result6DF_SQL = ${result6DF_SQL.count()}${RESET}")

    println(DIVIDER)

// --------------------------------------------------------------------------------------------------------------------------------------------

// 7(A). Find top 5 customer with highest number of orders

    println(s"${RED}TASK 7(A). Find top 5 customer with highest number of orders${RESET}")

    println(DFSTRING)

    val result7aDF = innerjoinedDF
      .groupBy("customer_id")
      .count()
      .sort(desc("count"), asc("customer_id"))
      .limit(5)

    result7aDF.show()

    println(SQLSTRING)

    val result7aDF_SQL = spark.sql(
      "SELECT customer_id, COUNT(*) AS count FROM joineddata GROUP BY customer_id ORDER BY count DESC, customer_id LIMIT 5"
    )

    result7aDF_SQL.show()

    println(DIVIDER)

// --------------------------------------------------------------------------------------------------------------------------------------------

// 7(B). Find top 5 customer with highest sum of total orders

    println(s"${RED}TASK 7(B). Find top 5 customer with highest sum of total orders${RESET}")

    println(DFSTRING)

    val result7bDF = innerjoinedDF
      .groupBy("customer_id")
      .agg(sum("order_id").as("order_sum"))
      .sort(desc("order_sum"), asc("customer_id"))
      .limit(5)

    result7bDF.show()

    println(SQLSTRING)

    val result7bDF_SQL = spark.sql(
      "SELECT customer_id, SUM(order_id) AS order_sum FROM joineddata GROUP BY customer_id ORDER BY order_sum DESC, customer_id LIMIT 5"
    )

    result7bDF_SQL.show()

    println(DIVIDER)

// --------------------------------------------------------------------------------------------------------------------------------------------

// 8. Find the customer who did not order in last 1 month or for long time

    println(s"${RED}TASK 8. Find the customer who did not order in last 1 month or for long time${RESET}")

    println(DFSTRING)

    val result8DF = innerjoinedDF
      .select("customer_id")
      .where(s"${datediff(current_date(), innerjoinedDF("order_date"))} >= 30")

    result8DF.show()
    println(s"${BLACK_B}${CYAN}Number of rows in result8DF = ${result8DF.count()}${RESET}")

    println(SQLSTRING)

    val result8DF_SQL = spark.sql(
      "SELECT customer_id FROM joineddata WHERE DATEDIFF(current_date(), order_date) >= 30"
    )

    result8DF_SQL.show()
    println(s"${BLACK_B}${CYAN}Number of rows in result8DF_SQL = ${result8DF_SQL.count()}${RESET}")

    println(DIVIDER)

// --------------------------------------------------------------------------------------------------------------------------------------------

// 9. Find the last order date for all customers

    println(s"${RED}TASK 9. Find the last order date for all customers${RESET}")

    println(DFSTRING)

    val result9DF = innerjoinedDF
      .groupBy("customer_id")
      .agg(max("order_date"))

    result9DF.show()
    println(s"${BLACK_B}${CYAN}Number of rows in result9DF = ${result9DF.count()}${RESET}")

    println(SQLSTRING)

    val result9DF_SQL = spark.sql(
      "SELECT customer_id, MAX(order_date) FROM joineddata GROUP BY customer_id"
    )

    result9DF_SQL.show()
    println(s"${BLACK_B}${CYAN}Number of rows in result9DF_SQL = ${result9DF_SQL.count()}${RESET}")

    println(DIVIDER)

// --------------------------------------------------------------------------------------------------------------------------------------------

// 10. Find open and close number of orders for a customer

    println(s"${RED}TASK 10. Find open and close number of orders for a customer${RESET}")

    println(DFSTRING)

    val open_orders = innerjoinedDF
      .where(s"customer_fname = '${fname}'")
      .where(s"customer_lname = '${lname}'")
      .where(innerjoinedDF("order_status").notEqual("CLOSED"))
      .count()

    val closed_orders = innerjoinedDF
      .where(s"customer_fname = '${fname}'")
      .where(s"customer_lname = '${lname}'")
      .where("order_status = 'CLOSED'")
      .count()

    println(s"open_orders = ${open_orders}\n")

    println(s"closed_orders = ${closed_orders}\n")

    println(SQLSTRING)

    val result10oDF_SQL = spark.sql(
      s"SELECT COUNT(*) AS open_orders FROM joineddata WHERE customer_fname = '${fname}' AND customer_lname = '${lname}' AND order_status != 'CLOSED'"
    )

    result10oDF_SQL.show()

    val result10cDF_SQL = spark.sql(
      s"SELECT COUNT(*) AS closed_orders FROM joineddata WHERE customer_fname = '${fname}' AND customer_lname = '${lname}' AND order_status = 'CLOSED'"
    )

    result10cDF_SQL.show()

    println(DIVIDER)

// --------------------------------------------------------------------------------------------------------------------------------------------

// 11. Find number of customers in every state

    println(s"${RED}TASK 11. Find number of customers in every state${RESET}")

    println(DFSTRING)

    val result11DF = customersDF
      .groupBy("customer_state")
      .count()

    result11DF.show(false)
    println(s"${BLACK_B}${CYAN}Number of rows in result11DF = ${result11DF.count()}${RESET}")

    println(SQLSTRING)

    val result11DF_SQL = spark.sql(
      "SELECT customer_state, COUNT(*) AS count FROM customers GROUP BY customer_state"
    )

    result11DF_SQL.show(false)
    println(s"${BLACK_B}${CYAN}Number of rows in result11DF_SQL = ${result11DF_SQL.count()}${RESET}")

    println(DIVIDER)

// --------------------------------------------------------------------------------------------------------------------------------------------

// 12. Find Number of customers in every city

    println(s"${RED}TASK 12. Number of customers in every city${RESET}")

    println(DFSTRING)

    val result12DF = customersDF
      .groupBy("customer_city")
      .count()

    result12DF.show(false)
    println(s"${BLACK_B}${CYAN}Number of rows in result12DF = ${result12DF.count()}${RESET}")

    println(SQLSTRING)

    val result12DF_SQL = spark.sql(
      "SELECT customer_city, COUNT(*) AS count FROM customers GROUP BY customer_city"
    )

    result12DF_SQL.show(false)
    println(s"${BLACK_B}${CYAN}Number of rows in result12DF_SQL = ${result12DF_SQL.count()}${RESET}")

    println(DIVIDER)

// --------------------------------------------------------------------------------------------------------------------------------------------

// 13. Latest 5 orders

    println(s"${RED}TASK 13. Latest 5 orders${RESET}")

    println(DFSTRING)

    val result13DF = ordersDF
      .orderBy(desc("order_date"))
      .limit(5)

    result13DF.show()

    println(SQLSTRING)

    val result13DF_SQL = spark.sql(
      "SELECT * FROM orders ORDER BY order_date DESC LIMIT 5"
    )

    result13DF_SQL.show()

    println(DIVIDER)

// --------------------------------------------------------------------------------------------------------------------------------------------

// 14. Count of orders based on city

    println(s"${RED}TASK 14. Count of orders based on city${RESET}")

    println(DFSTRING)

    val result14DF = innerjoinedDF
      .groupBy("customer_city")
      .agg(count("order_id")).as("count_of_orders")

    result14DF.show()
    println(s"${BLACK_B}${CYAN}Number of rows in result14DF = ${result14DF.count()}${RESET}")

    println(SQLSTRING)

    val result14DF_SQL = spark.sql(
      "SELECT customer_city, COUNT(order_id) AS count_of_orders FROM joineddata GROUP BY customer_city"
    )

    result14DF_SQL.show()
    println(s"${BLACK_B}${CYAN}Number of rows in result14DF_SQL = ${result14DF_SQL.count()}${RESET}")

    println(DIVIDER)

// --------------------------------------------------------------------------------------------------------------------------------------------

    println(s"${BLACK_B}${CYAN}Success!${RESET}")
//    Thread.sleep(30000)

    sc.stop()
    spark.stop()
  }
}
