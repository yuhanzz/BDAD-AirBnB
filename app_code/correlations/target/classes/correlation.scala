import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.col

trait sparkEnv {
  var spark: SparkSession = _
  var sc: SparkContext = _
}

object Correlation extends sparkEnv {

  def main(args: Array[String]) {

    spark =
      SparkSession.builder().appName("Airbnb Cross Correlation").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val file_path_311 =
      "hdfs:///user/lh2978/project/data/listings/datasets/clean_nyc311"
    val file_path_airbnb =
      "hdfs:///user/lh2978/project/data/listings/datasets/combined_header_updated"
    val file_path_real_estate =
      "hdfs:///user/lh2978/project/data/listings/datasets/real_estate_cleaned_data"
    val file_path_real_estate_for_average =
      "hdfs:///user/yz6238/rdba_project/real_estate_preprocessed_data/*.csv/*.csv"

    // get monthly average data and print
    val df_311_for_average =
      get_311_counts(file_path_311, 0)
        .select("key", "all_type_complaint")
    val df_real_estate_for_average =
      get_real_estate_average(file_path_real_estate_for_average, 0)
    val df_airbnb_for_average = get_airbnb_average_price(file_path_airbnb)

    println(s"monthly average:\n")
    val df_joined_for_average = df_airbnb_for_average
      .join(df_311_for_average, "key")
      .join(df_real_estate_for_average, "key")
    df_joined_for_average.collect.foreach(println)

    // get correlations and print
    println(s"cross correlations:\n")

    for (i <- 0 to 6) {
      val df_311 = process_311_by_time_shift(file_path_311, i)
      val df_airbnb = process_airbnb(file_path_airbnb)
      val df_real_estate =
        process_real_estate_by_time_shift(file_path_real_estate, i)

      val corrs = cross_correlation(df_311, df_airbnb, df_real_estate)
      val corr_real_estate = corrs._1
      val corr_311 = corrs._2
      println(
        s"time shift $i, real estate correlation: $corr_real_estate, 311 correlation: $corr_311 \n"
      )
    }

    sc.stop()
  }

  // return a tuple of the correlation between the airbnb price and real estate price, and the correlation between the airbnb price and all type complaint count
  def cross_correlation(
      df_311: DataFrame,
      df_airbnb: DataFrame,
      df_real_estate: DataFrame
  ) = {
    val df_joined =
      df_311.join(df_real_estate, "key").join(df_airbnb, "key")
    (
      df_joined.stat.corr("price", "real_estate_price"),
      df_joined.stat.corr("price", "all_type_complaint")
    )
  }

  // get the total counts of complaints of different types, could specify lead or lag by month_shift
  // different from the process_311_by_time_shift function, we sum the counts in all zipcodes
  // sample: [2016-06,188697,20838,2466,11539,10354,8557,0,3786,5423,4639,0]
  def get_311_counts(file_path: String, month_shift: Int) = {
    val data_raw_311 = sc.textFile(file_path)
    val date_splitted_311 =
      data_raw_311.map(row => row.substring(1, row.length - 1).split(','))
    val data_selected_311 =
      date_splitted_311.map(row => (row(1), row(3), row(4)))
    val data_date_formatted =
      data_selected_311.map(row => (format_date(row._1), row._2, row._3))

    val data_complaint_transformed =
      data_date_formatted.map(row => (row._1, transform_complaint_type(row._2)))

    val data_by_month = data_complaint_transformed.reduceByKey((v1, v2) =>
      (
        v1._1 + v2._1,
        v1._2 + v2._2,
        v1._3 + v2._3,
        v1._4 + v2._4,
        v1._5 + v2._5,
        v1._6 + v2._6,
        v1._7 + v2._7,
        v1._8 + v2._8,
        v1._9 + v2._9,
        v1._10 + v2._10,
        v1._11 + v2._11
      )
    )
    val data_flat_311 = data_by_month.map(row =>
      (
        row._1,
        row._2._1,
        row._2._2,
        row._2._3,
        row._2._4,
        row._2._5,
        row._2._6,
        row._2._7,
        row._2._8,
        row._2._9,
        row._2._10,
        row._2._11
      )
    )

    val shifted_311 =
      if (month_shift < 0)
        data_flat_311.map(row =>
          (
            lead_multiple_month(row._1, -month_shift),
            row._2,
            row._3,
            row._4,
            row._5,
            row._6,
            row._7,
            row._8,
            row._9,
            row._10,
            row._11,
            row._12
          )
        )
      else
        data_flat_311.map(row =>
          (
            lag_multiple_month(row._1, month_shift),
            row._2,
            row._3,
            row._4,
            row._5,
            row._6,
            row._7,
            row._8,
            row._9,
            row._10,
            row._11,
            row._12
          )
        )

    val schema_311 =
      StructType(
        Array(
          StructField("key", StringType, nullable = false),
          StructField("all_type_complaint", IntegerType, nullable = true),
          StructField("noise_residential", IntegerType, nullable = true),
          StructField("hot_water", IntegerType, nullable = true),
          StructField("illegal_parking", IntegerType, nullable = true),
          StructField("blocked_driveway", IntegerType, nullable = true),
          StructField("street_condition", IntegerType, nullable = true),
          StructField("heating", IntegerType, nullable = true),
          StructField("plumbing", IntegerType, nullable = true),
          StructField("water_system", IntegerType, nullable = true),
          StructField("street_light_condition", IntegerType, nullable = true),
          StructField("general_construction", IntegerType, nullable = true)
        )
      )

    val row_rdd_311 =
      shifted_311.map(row => Row.fromSeq(row.productIterator.toSeq))
    val sqlCtx = new SQLContext(sc)
    import sqlCtx._
    val df_311 = sqlCtx.createDataFrame(row_rdd_311, schema_311)
    df_311
  }

  // get the average airbnb price by month e.g. [2018-06,137.91308427157966]
  def get_airbnb_average_price(file_path: String) = {
    val df_raw_airbnb = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(file_path)

    val df_price_date = df_raw_airbnb.select("Date", "price")
    val rdd_airbnb = df_price_date.rdd
    val rdd_formatted = rdd_airbnb.map(row =>
      (
        row(0).toString.split('/')(0) + "-" + row(0).toString.split('/')(1),
        (row(1).asInstanceOf[Double], 1)
      )
    )

    val rdd_month_aggregated =
      rdd_formatted.reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))

    val rdd_month_average =
      rdd_month_aggregated.map(row => (row._1, row._2._1 / row._2._2))

    val schema_airbnb =
      StructType(
        Array(
          StructField("key", StringType, nullable = false),
          StructField("price", DoubleType, nullable = true)
        )
      )

    val sqlCtx = new SQLContext(sc)
    import sqlCtx._
    val df_airbnb = sqlCtx.createDataFrame(
      rdd_month_average.map(row => Row.fromSeq(row.productIterator.toSeq)),
      schema_airbnb
    )
    df_airbnb
  }

  // get the average real estate price by month e.g. (2017-12,982.3736040084256), could specify lead or lag by month_shift
  def get_real_estate_average(file_path: String, month_shift: Int) = {

    val raw_rdd = sc.textFile(file_path)
    val rdd_separated = raw_rdd.map(row => row.split(';'))
    val rdd_selected_cols =
      rdd_separated.map(row => (row(10), row(15), row(19), row(20)))

    val rdd_removed_null = rdd_selected_cols.filter(row =>
      row._1 != "" && row._2 != "" && row._3 != "" && row._4 != ""
    )
    val rdd_format_num = rdd_removed_null.map(row =>
      (
        row._1.trim(),
        row._2.replaceAll(",", "").trim(),
        row._3.replaceAll("[$,]", "").trim(),
        row._4.trim()
      )
    )
    val rdd_format_num2 = rdd_format_num.map(row =>
      (
        row._1,
        row._2.replaceAll("-", "").trim(),
        row._3.replaceAll("-", "").trim(),
        row._4
      )
    )
    val rdd_with_num = rdd_format_num2.map(row =>
      (row._1, row._2.toDouble, row._3.toDouble, row._4)
    )
    val rdd_check_square_feet = rdd_with_num.filter(row => row._2 > 0)
    val rdd_check_sale_price = rdd_check_square_feet.filter(row => row._3 > 0)
    val rdd_check_zipcode =
      rdd_check_sale_price.filter(row => row._1.length == 5)
    val rdd_date_formatted =
      rdd_check_zipcode.map(row =>
        (row._1, row._2, row._3, format_date(row._4))
      )
    val rdd_pair =
      rdd_date_formatted.map(row => Tuple2(row._4, (row._3 / row._2, 1)))
    val rdd_month_aggregated =
      rdd_pair.reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
    val rdd_month_average =
      rdd_month_aggregated.map(row => (row._1, row._2._1 / row._2._2))

    val rdd_shifted =
      if (month_shift < 0)
        rdd_month_average.map(row =>
          (lead_multiple_month(row._1, -month_shift), row._2)
        )
      else
        rdd_month_average.map(row =>
          (lag_multiple_month(row._1, month_shift), row._2)
        )

    val schema_real_estate =
      StructType(
        Array(
          StructField("key", StringType, nullable = false),
          StructField("real_estate_price", DoubleType, nullable = true)
        )
      )
    val sqlCtx = new SQLContext(sc)
    import sqlCtx._
    val df_real_estate =
      sqlCtx.createDataFrame(
        rdd_shifted.map(row => Row.fromSeq(row.productIterator.toSeq)),
        schema_real_estate
      )
    df_real_estate
  }

  // get the 311 dataframe, the format of each row is (date|zipcode, all_type_complaint count)
  // the date is led or lagged according to "month_shift" arg, negative number mean lead and positive number means lag
  def process_311_by_time_shift(file_path: String, month_shift: Int) = {
    val data_raw_311 = sc.textFile(file_path)
    val date_splitted_311 =
      data_raw_311.map(row => row.substring(1, row.length - 1).split(','))
    val data_selected_311 =
      date_splitted_311.map(row => (row(1), row(3), row(4)))
    val data_date_formatted =
      data_selected_311.map(row => (format_date(row._1), row._2, row._3))

    val shifted_311 =
      if (month_shift < 0)
        data_date_formatted.map(row =>
          (
            lead_multiple_month(row._1, -month_shift),
            row._2,
            row._3
          )
        )
      else
        data_date_formatted.map(row =>
          (
            lag_multiple_month(row._1, month_shift),
            row._2,
            row._3
          )
        )

    val data_complaint_transformed = shifted_311.map(row =>
      (row._1 + "|" + row._3, transform_complaint_type(row._2))
    )

    val data_by_month = data_complaint_transformed.reduceByKey((v1, v2) =>
      (
        v1._1 + v2._1,
        v1._2 + v2._2,
        v1._3 + v2._3,
        v1._4 + v2._4,
        v1._5 + v2._5,
        v1._6 + v2._6,
        v1._7 + v2._7,
        v1._8 + v2._8,
        v1._9 + v2._9,
        v1._10 + v2._10,
        v1._11 + v2._11
      )
    )
    val data_flat_311 = data_by_month.map(row =>
      (
        row._1,
        row._2._1,
        row._2._2,
        row._2._3,
        row._2._4,
        row._2._5,
        row._2._6,
        row._2._7,
        row._2._8,
        row._2._9,
        row._2._10,
        row._2._11
      )
    )

    val schema_311 =
      StructType(
        Array(
          StructField("key", StringType, nullable = false),
          StructField("all_type_complaint", IntegerType, nullable = true),
          StructField("noise_residential", IntegerType, nullable = true),
          StructField("hot_water", IntegerType, nullable = true),
          StructField("illegal_parking", IntegerType, nullable = true),
          StructField("blocked_driveway", IntegerType, nullable = true),
          StructField("street_condition", IntegerType, nullable = true),
          StructField("heating", IntegerType, nullable = true),
          StructField("plumbing", IntegerType, nullable = true),
          StructField("water_system", IntegerType, nullable = true),
          StructField("street_light_condition", IntegerType, nullable = true),
          StructField("general_construction", IntegerType, nullable = true)
        )
      )

    val row_rdd_311 =
      data_flat_311.map(row => Row.fromSeq(row.productIterator.toSeq))
    val sqlCtx = new SQLContext(sc)
    import sqlCtx._
    val df_311 = sqlCtx.createDataFrame(row_rdd_311, schema_311)
    df_311
  }

  // get the airbnb price dataframe, the format of each row is (date|zipcode, airbnb price)
  def process_airbnb(file_path: String) = {
    val df_raw_airbnb = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(file_path)

    val df_price_date = df_raw_airbnb.select("Date", "price", "zipcode")
    val rdd_airbnb = df_price_date.rdd
    val rdd_formatted = rdd_airbnb.map(row =>
      (
        row(0).toString.split('/')(0) + "-" + row(0).toString
          .split('/')(1) + "|" + row(2),
        row(1).asInstanceOf[Double]
      )
    )

    val schema_airbnb =
      StructType(
        Array(
          StructField("key", StringType, nullable = false),
          StructField("price", DoubleType, nullable = true)
        )
      )

    val sqlCtx = new SQLContext(sc)
    import sqlCtx._
    val df_airbnb = sqlCtx.createDataFrame(
      rdd_formatted.map(row => Row.fromSeq(row.productIterator.toSeq)),
      schema_airbnb
    )
    df_airbnb
  }

  // get the real estate dataframe, the format of each row is (date|zipcode, real_estate_price)
  // the date is led or lagged according to "month_shift" arg, negative number mean lead and positive number means lag
  def process_real_estate_by_time_shift(file_path: String, month_shift: Int) = {
    val rdd_real_estate = sc.textFile(file_path).map(row => row.split(','))

    val rdd_shifted =
      if (month_shift < 0)
        rdd_real_estate.map(row =>
          (lead_multiple_month(row(0), -month_shift), row(1), row(2))
        )
      else
        rdd_real_estate.map(row =>
          (lag_multiple_month(row(0), month_shift), row(1), row(2))
        )

    val rdd_real_estate_get_key =
      rdd_shifted.map(row => (row._1 + "|" + row._2, row._3.toFloat))

    val schema_real_estate =
      StructType(
        Array(
          StructField("key", StringType, nullable = false),
          StructField("real_estate_price", FloatType, nullable = true)
        )
      )
    val sqlCtx = new SQLContext(sc)
    import sqlCtx._
    val df_real_estate =
      sqlCtx.createDataFrame(
        rdd_real_estate_get_key.map(row =>
          Row.fromSeq(row.productIterator.toSeq)
        ),
        schema_real_estate
      )
    df_real_estate
  }

  // format the date (e.g 10/31/2018 => 2018-10)
  def format_date(old_date: String): String = {
    val fields = old_date.split("/")
    val month = if (fields(0).length == 2) fields(0) else "0" + fields(0)
    val year = fields(2).length match {
      case 1 => "200" + fields(2)
      case 2 => "20" + fields(2)
      case 4 => fields(2)
    }
    year + "-" + month
  }

  // lead the date by multiple month, e.g. (2012-09, 3) => 2012-06
  def lead_multiple_month(old_date: String, months: Int) = {
    var new_date = old_date
    for (i <- 1 to months) {
      new_date = lead_date(new_date)
    }
    new_date
  }

  // lag the date by multiple month, e.g. (2012-01, 3) => 2012-04
  def lag_multiple_month(old_date: String, months: Int) = {
    var new_date = old_date
    for (i <- 1 to months) {
      new_date = lag_date(new_date)
    }
    new_date
  }

  // lead the date (e.g. 2010-01 => 2009-12)
  def lead_date(old_date: String) = {
    val old_month = old_date.split('-')(1)
    val old_year = old_date.split('-')(0)
    val new_month = if (old_month.toInt == 1) 12 else old_month.toInt - 1
    val new_year =
      if (old_month.toInt == 1) old_year.toInt - 1 else old_year.toInt
    val formatted_month =
      if (new_month.toString.length == 2) new_month.toString
      else "0" + new_month
    new_year + "-" + formatted_month
  }

  // lag the date (e.g. 2010-12 => 2011-01)
  def lag_date(old_date: String) = {
    val old_month = old_date.split('-')(1)
    val old_year = old_date.split('-')(0)
    val new_month = if (old_month.toInt == 12) 1 else old_month.toInt + 1
    val new_year =
      if (old_month.toInt == 12) old_year.toInt + 1 else old_year.toInt
    val formatted_month =
      if (new_month.toString.length == 2) new_month.toString
      else "0" + new_month
    new_year + "-" + formatted_month
  }

  // transform complaint type field to one-hot vector
  def transform_complaint_type(complaint_type: String) = {
    val return_tuple = complaint_type match {
      case "Noise - Residential"    => (1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0)
      case "HEAT/HOT WATER"         => (1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0)
      case "Illegal Parking"        => (1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0)
      case "Blocked Driveway"       => (1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0)
      case "Street Condition"       => (1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0)
      case "HEATING"                => (1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0)
      case "PLUMBING"               => (1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0)
      case "Water System"           => (1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0)
      case "Street Light Condition" => (1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0)
      case "GENERAL CONSTRUCTION"   => (1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1)
      case default                  => (1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    }
    return_tuple
  }

}
