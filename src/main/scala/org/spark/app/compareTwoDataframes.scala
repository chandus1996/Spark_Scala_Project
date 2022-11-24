package org.spark.app

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array, col, when}


object compareTwoDataframes {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    import spark.implicits._

    //    options is not working not sure why?
    //    val dataframe_mysql = spark.read.format("jdbc").options(
    //      val url = "jdbc:mysql://localhost:3306/my_bd_name",
    //      val driver = "com.mysql.jdbc.Driver",
    //      val dbtable = "my_tablename",
    //      val user = "root",
    //      val password = "root").load()

    val dataframe_mysql = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/world")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "city")
      .option("user", "root")
      .option("password", "root").load()
    dataframe_mysql.show()
    //  practical
    val lastdata = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/world")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("query", "select * from city")
      .option("user", "root")
      .option("password", "root")
      .load();
    lastdata.show()

    println("============================")
    //    val dirpath1=args(0)
    //    val dirpath2=args(1)
    val dirpath3 = "../TestData/file2.csv"
    val dirpath4 = "../TestData/file22.csv"
    //    val df1 = spark.read.option("header", "true").csv(s"{dirpath3}");
    //    val df2 = spark.read.option("header", "true").csv(s"{dirpath4}");

    val df1 = spark.read.option("header", "true").csv("D:\\hadoop_practise\\compareTwoDFs\\TestData\\file2.csv");
    val df2 = spark.read.option("header", "true").csv("D:\\hadoop_practise\\compareTwoDFs\\TestData\\file22.csv");
    df1.show()
    println("============================")
    df2.show()

    println("============================")
    println("Records which are not same in both dataframes would be fetched")
    println("Records updated old and new records both would be fetched\nRecords added in left table will dispaly\nRecords added in right table will display")
    val finaldf = df1.alias("d1").join(df2.alias("d2"), Seq("id", "ename", "sal"), "outer").where(col("d2.id").isNull || col("d1.id").isNull)
    finaldf.show()

    println("============================")
    println("Record contains in the left table if updated then it will load in this dataframe")
    val cols = df1.columns.filter(_ != "id").toList
    println(cols)
    val df3 = df1.except(df2)

    def mapDiffs(name: String) = when($"l.$name" === $"r.$name", null).otherwise(array($"l.$name", $"r.$name")).as(name)

    val updatedRecords = df2.as("l").join(df3.as("r"), "id").select($"id" :: cols.map(mapDiffs): _*)
    updatedRecords.show(false)

    println("============================")
    println("Compare dataframe column differences")
    val list_col = List()
    val colss = df1.columns
    for (i <- colss)
      println(i + " ")
    println()

    val columns = df1.schema.fields.map(_.name)
    val selectiveDifferences = columns.map(col => df1.select(col).except(df2.select(col)))

    selectiveDifferences.map(diff => {
      if (diff.count > 0) diff.show
    })
    println()
    //
    val selectdiff1 = columns.map(col => df1.select(col).except(df2.select(col)))
    selectdiff1.map(diff => {
      if (diff.count > 0) diff.show
    })
    val selectdiff2 = columns.map(col => df2.select(col).except(df1.select(col)))
    selectdiff2.map(diff2 => {
      if (diff2.count > 0) diff2.show
    })

    finaldf.write.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/world")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "employeeDetails")
      .option("user", "root")
      .option("password", "root")
      .mode("append")
      .saveAsTable("employeeDetails")
    //    updatedRecords.show()

    println("dataframe written to mysql server")


  }

}