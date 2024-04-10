package org.goncalo.application

import org.apache.spark.sql.{Row, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs._
import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr, StringToAttributeConversionHelper}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, first}

import scala.::
import scala.language.postfixOps

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Xpand-challenge")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val df_reviews = spark.read
      .option("header", value = true)

      .csv("data/googleplaystore_user_reviews.csv")

    val df_apps = spark.read
      .option("header", value = true)
      .csv("data/googleplaystore.csv")

    var df_1 = df_reviews.withColumn("Sentiment_Polarity", col("Sentiment_Polarity").cast(DoubleType))
      .withColumn("Sentiment_Subjectivity", col("Sentiment_Subjectivity").cast(DoubleType))

    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")


    df_1 = df_1.na.fill(0, Array("Sentiment_polarity"))
      .groupBy("App")
      .agg(functions.avg("Sentiment_polarity").as("Average_Sentiment_Polarity"))

    val df_2 = df_apps.withColumn("Rating", col("Rating").cast(DoubleType))

    val apps = df_2("App")
    val rating = df_2("Rating")

    val modified_df = df_2.select(apps, rating)
      .na.fill(0, Array("Rating"))
      .filter(rating >= 4.0)
      .orderBy(col("Rating").desc)

    modified_df
      .write
      .option("header", value = true)
      .option("delimiter", "§")
      .csv("data/best_apps")

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    // Get the name of the written csv file
    val fileName = fs.globStatus(new Path("data/best_apps/*.csv"))(0).getPath.getName

    // Rename the file
    fs.rename(new Path("data/best_apps/" + fileName), new Path("data/best_apps.csv"))

    // Delete the temporary directory
    fs.delete(new Path("data/best_apps"), true)


    var df = df_apps

    df = df.withColumn("Genres", split(df("Genres"), ";"))
      .withColumn("Rating", col("Rating").cast(DoubleType))
    df.show()


    val takesDollarSign = udf((s: String) => {

      if (s.startsWith("$")) {
        s.stripPrefix("$")
      }
      else {
        s
      }
    })

    var new_data_frame = df.filter(!(col("Size") === "Varies with device") && !(col("Size") === "HEALTH_AND_FITNESS") && !(col("Size") === "1,000+"))
    new_data_frame = new_data_frame.withColumn("Price", takesDollarSign(col("price")))
    new_data_frame.show()

    val convertSizetoDouble = udf((s: String) => {
      if (s.endsWith("M")) {
        s.stripSuffix("M").toDouble
      } else if (s.endsWith("k")) {
        s.stripSuffix("k").toDouble / 1024
      }
      else {
        s.toDouble
      }

    })
    new_data_frame = new_data_frame.withColumnRenamed("Content Rating", "Content_Rating")
      .withColumnRenamed("Last Updated", "Last_Updated")
      .withColumnRenamed("Current Ver", "Current_Version")
      .withColumnRenamed("Android Ver", "Minimum_Android_Version")

    new_data_frame = new_data_frame.withColumn("Size", convertSizetoDouble(col("Size")))
    new_data_frame = new_data_frame.withColumn("Price", col("Price").cast(DoubleType))
    new_data_frame = new_data_frame.withColumn("Price", new_data_frame("Price") * 0.9)
    new_data_frame = new_data_frame.withColumn("Last_Updated", to_date(col("Last_Updated"), "MMMM d,yyyy"))
    new_data_frame = new_data_frame.withColumn("Last_Updated", date_format(col("Last_Updated"), "YYYY-MM-d HH:mm:ss"))
      .na.fill(0, Array("Rating"))
      .withColumn("Reviews", col("Reviews").cast(LongType))


    var temp_view = new_data_frame.groupBy("App")
      .agg(
        collect_set("Category").as("Categories")
        , functions.max("Reviews").as("Reviews"),
        first("Rating").as("Rating"),
        first("Size").as("Size"),
        first("Installs").as("Installs"),
        first("Type").as("Type"),
        first("Price").as("Price"),
        first("Content_Rating").as("Content_Rating"),
        first("Genres").as("Genres"),
        first("Last_Updated").as("Last_Updated"),
        first("Current_Version").as("Current_Version"),
        first("Minimum_Android_Version").as("Minimum_Android_Version")
      )

    temp_view.filter(col("App") === "Clash of Clans").show()

    var reoder_coluns = List("App", "Categories", "Rating", "Reviews", "Size", "Installs", "Type", "Price", "Content_Rating", "Genres", "Last_Updated", "Current_Version", "Minimum_Android_Version")

    val df_3 = temp_view.select(reoder_coluns.head, reoder_coluns.tail: _*)

    reoder_coluns = reoder_coluns :+ "Average_Sentiment_Polarity"
    var part_4 = df_1.join(df_3, "App", "Inner").dropDuplicates("App")

    val save_part_4 = part_4.select(reoder_coluns.head, reoder_coluns.tail: _*)
    save_part_4.write
      .option("compression", "gzip")
      .parquet("data/googleplaystore_cleaned")


    // Get the name of the written csv file
    val fileName2 = fs.globStatus(new Path("data/googleplaystore_cleaned/*.parquet"))(0).getPath.getName

    // Rename the file
    fs.rename(new Path("data/googleplaystore_cleaned/" + fileName2), new Path("data/googleplaystore_cleaned.parquet"))

    // Delete the temporary directory
    fs.delete(new Path("data/googleplaystore_cleaned"), true)


    var df_4 = df_3

    df_4 = df_4.withColumn("Genres", explode(col("Genres").as("Genre"))).dropDuplicates("App")
      .withColumnRenamed("Genres", "Genre")
      .na.fill(0, Array("Rating"))
    df_4 = df_4.join(df_reviews.select("App", "Sentiment_Polarity"), "App")
    df_4.show()
    df_4 = df_4.withColumn("Sentiment_Polarity", col("Sentiment_Polarity").cast(DoubleType))
      .na.fill(0, Array("Sentiment_Polarity"))


    df_4 = df_4
      .groupBy("Genre")
      .agg(
        functions.countDistinct("App").as("Count"),
        functions.avg("Rating").as("Average_Rating"),
        functions.avg("Sentiment_Polarity").as("Average_Sentiment_Polarity")
      )

    df_4.write
      .option("compression", "gzip")
      .parquet("data/googleplaystore_metrics")


    // Get the name of the written csv file
    val fileName3 = fs.globStatus(new Path("data/googleplaystore_metrics/*.parquet"))(0).getPath.getName

    // Rename the file
    fs.rename(new Path("data/googleplaystore_metrics/" + fileName3), new Path("data/googleplaystore_metrics.parquet"))

    // Delete the temporary directory
    fs.delete(new Path("data/googleplaystore_metrics"), true)


  }
}