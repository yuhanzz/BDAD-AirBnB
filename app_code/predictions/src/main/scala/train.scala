import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext

import org.apache.spark.ml._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
//import ml.dmlc.xgboost4j.scala.spark.XGBoostRegressor
//import com.microsoft.ml.spark.lightgbm.LightGBMRegressor
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.{
  RandomForestRegressionModel,
  RandomForestRegressor
}
import org.apache.spark.ml.feature.MinMaxScaler


trait sparkEnv {
  var spark: SparkSession = _
  var sc: SparkContext = _
}

object TrainModel extends sparkEnv {

  def main(args: Array[String]) {

    spark = SparkSession.builder().appName("Airbnb Prediction").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    // data prep
    val file_path_311 =
      "hdfs:///user/lh2978/project/data/listings/datasets/clean_nyc311"
    val file_path_airbnb =
      "hdfs:///user/lh2978/project/data/listings/datasets/combined_header_updated"
    val file_path_real_estate =
      "hdfs:///user/lh2978/project/data/listings/datasets/real_estate_cleaned_data"

    val df_311 = process_311_version2(file_path_311)
    val df_airbnb = process_airbnb_version1(file_path_airbnb)
    val df_real_estate = process_real_estate_version2(file_path_real_estate)

    // join all data, key column format: 2018-10|11418
    val df_joined =
      df_311.join(df_real_estate, "key").join(df_airbnb, "key").drop("key")

    df_joined.printSchema
    //df_joined.show(1)

    // follwing are the normalization steps
    // we did not incorporate this step in the final version because it excessively extended the training time
    /*
    println("begin to normalize features:")
    // normalize data
    val columns_to_scale = df_joined.columns
    var col = ""
    var assemblers = Array[VectorAssembler]()
    for (col <- columns_to_scale) {
      if (col != "price") {
        assemblers = assemblers ++ Array(
          new VectorAssembler()
            .setInputCols(Array(col))
            .setOutputCol(col + "_vec")
        )
      }
    }
    var scalers = Array[MinMaxScaler]()
    for (col <- columns_to_scale) {
      if (col != "price") {
        scalers = scalers ++ Array(
          new MinMaxScaler()
            .setInputCol(col + "_vec")
            .setOutputCol(col + "_scaled")
        )
      }
    }
    val pipelines = new Pipeline().setStages(assemblers ++ scalers)
    val scaleModel = pipelines.fit(df_joined)
    val combinedData = scaleModel.transform(df_joined)

    var selectedCols = Array[String]()
    for (col <- columns_to_scale) {
      if (col == "price") {
        selectedCols = selectedCols ++ Array("price")
      } else {
        selectedCols = selectedCols ++ Array(col + "_scaled")
      }
    }
    val selectedSeq = selectedCols.toSeq
    var scaledData = combinedData.select(selectedSeq.head, selectedSeq.tail: _*)
    for (col <- selectedCols) {
      if (col != "price") {
        scaledData =
          scaledData.withColumnRenamed(col, col.substring(0, col.length - 7))
      }
    }
    scaledData.schema
    scaledData.show(1)

    test_lr(scaledData)
    test_gbt(scaledData)
    test_rff(scaledData)
    */

   test_lr(df_joined)
   test_gbt(df_joined)
   test_rff(df_joined)
    println("finished")

    sc.stop()
  }

  // train and test the linear regression model using the joined dataframe
  // output the rmse and mae of the model 
  // save the model to hdfs
  def test_lr(data: DataFrame) = {
    // train test split
    val Array(training_set, test_set) =
      data.randomSplit(Array(0.9, 0.1), seed = 12345)
    // assembler change columns to feature vector
    // the arg of setInputCols should be an array of feature names such as Array("bathrooms", "beds", "property_is_Hotel")
    // here I'm using all the columns except the last column(price) as features
    val assembler = new VectorAssembler()
      .setInputCols(data.columns.take(35) ++ data.columns.drop(35 + 1))
      .setOutputCol("features")
    // you can get other metrics by changing the parameter of setMetricName
    val rmse_evaluator = new RegressionEvaluator()
      .setLabelCol("price")
      .setPredictionCol("prediction")
      .setMetricName("rmse");
     val mae_evaluator = new RegressionEvaluator()
      .setLabelCol("price")
      .setPredictionCol("prediction")
      .setMetricName("mae");

    // change the model-specific part here
    val lr = new LinearRegression()
      .setMaxIter(100)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setFeaturesCol("features")
      .setLabelCol("price")
    val pipeline = new Pipeline().setStages(Array(assembler, lr))

    // train on training set
    val model = pipeline.fit(training_set)
    // evaluate on test set and print
    val predictions = model.transform(test_set)
    val rmse = rmse_evaluator.evaluate(predictions);
    val mae = mae_evaluator.evaluate(predictions);
    // change the text here
    println(s"linear regression rmse: ${rmse}")
    println(s"LR MAE: ${mae}")
    model.write.overwrite().save("rdba_project/linear_regression_model")
  }

  // train and test the gradient-boosted regression model using the joined dataframe
  // output the rmse and mae of the model 
  // save the model to hdfs
  def test_gbt(data: DataFrame) = {
    val Array(training_set, test_set) =
      data.randomSplit(Array(0.9, 0.1), seed = 12345)
    val assembler = new VectorAssembler()
      .setInputCols(data.columns.take(35) ++ data.columns.drop(35 + 1))
      .setOutputCol("features")
    val rmse_evaluator = new RegressionEvaluator()
      .setLabelCol("price")
      .setPredictionCol("prediction")
      .setMetricName("rmse");
   val mae_evaluator = new RegressionEvaluator()
      .setLabelCol("price")
      .setPredictionCol("prediction")
      .setMetricName("mae");

    // model-specific part
    val gbt = new GBTRegressor()
      .setLabelCol("price")
      .setFeaturesCol("features")
      .setMaxIter(50)
    val pipeline = new Pipeline().setStages(Array(assembler, gbt))

    val model = pipeline.fit(training_set)
    val predictions = model.transform(test_set)
    val rmse = rmse_evaluator.evaluate(predictions);
    val mae = mae_evaluator.evaluate(predictions);
    println(s"gradient-boosted tree regression rmse: ${rmse}")
    println(s"GBT mae: ${mae}")
    model.write.overwrite().save("rdba_project/gbt_model")

  }

  // This function was not adoppted due to the uncompatible maven dependency
  // def test_xgboost(data:DataFrame) = {
  //  val Array(training_set, test_set) =
  //     data.randomSplit(Array(0.9, 0.1), seed = 12345)
  //   val assembler = new VectorAssembler()
  //     .setInputCols(data.columns.take(35) ++ data.columns.drop(35 + 1))
  //     .setOutputCol("features")
  //   val rmse_evaluator = new RegressionEvaluator()
  //     .setLabelCol("price")
  //     .setPredictionCol("prediction")
  //     .setMetricName("rmse");
  //   val xgbParam = Map("eta" -> 0.3,
  //     "max_depth" -> 6,
  //     "objective" -> "reg:squarederror",
  //     "num_round" -> 10,
  //     "num_workers" -> 2)

  //     val xgboost = new XGBoostRegressor(xgbParam).setLabelCol("price").setFeaturesCol("features")
  //     val xgbRegressionModel = xgboost.fit(training_set)
  //     val predictions = xgbRegressionModel.transform(test_set)
  //     val rmse = rmse_evaluator.evaluate(predictions);
  //     println("XGboost regression rmse: ${rmse}")
  // }

  // train and test the random forest model using the joined dataframe
  // output the rmse and mae of the model 
  // save the model to hdfs
  def test_rff(data: DataFrame) = {

    val Array(training_set, test_set) =
      data.randomSplit(Array(0.9, 0.1), seed = 12345)
    // assembler change columns to feature vector
    // the arg of setInputCols should be an array of feature names such as Array("bathrooms", "beds", "property_is_Hotel")
    // here I'm using all the columns except the last column(price) as features
    val assembler = new VectorAssembler()
      .setInputCols(data.columns.take(35) ++ data.columns.drop(35 + 1))
      .setOutputCol("features")
    // you can get other metrics by changing the parameter of setMetricName
    val rmse_evaluator = new RegressionEvaluator()
      .setLabelCol("price")
      .setPredictionCol("prediction")
      .setMetricName("rmse");

    val mae_evaluator = new RegressionEvaluator()
      .setLabelCol("price")
      .setPredictionCol("prediction")
      .setMetricName("mae");

    // change the model-specific part here
    val rff = new RandomForestRegressor()
      .setMaxDepth(20)
      .setNumTrees(10)
      .setFeaturesCol("features")
      .setLabelCol("price")
    val pipeline = new Pipeline().setStages(Array(assembler, rff))

    // train on training set
    val model = pipeline.fit(training_set)
    // evaluate on test set and print
    val predictions = model.transform(test_set)
    val rmse = rmse_evaluator.evaluate(predictions);
    val mae = mae_evaluator.evaluate(predictions);
    model.write.overwrite().save("project/data/listings/datasets/random_forest_model")
    // change the text here
    println(s"random forest rmse: ${rmse}")
    println(s"random forest mae: ${mae}")
    println("finished")

  }

  // This function was not adoppted due to the uncompatible maven dependency
  // def test_lgbm(data: DataFrame) = {

  //    val Array(training_set, test_set) =
  //     data.randomSplit(Array(0.9, 0.1), seed = 12345)
  //   // assembler change columns to feature vector
  //   // the arg of setInputCols should be an array of feature names such as Array("bathrooms", "beds", "property_is_Hotel")
  //   // here I'm using all the columns except the last column(price) as features
  //   val assembler = new VectorAssembler()
  //     .setInputCols(data.columns.take(35) ++ data.columns.drop(35 + 1))
  //     .setOutputCol("features")
  //   // you can get other metrics by changing the parameter of setMetricName
  //   val rmse_evaluator = new RegressionEvaluator()
  //     .setLabelCol("price")
  //     .setPredictionCol("prediction")
  //     .setMetricName("rmse");

  //   // change the model-specific part here
  //   val rff = new LightGBMRegressor()
  //     .setMaxDepth(10)
  //     .setFeaturesCol("features")
  //     .setLabelCol("price")

  //   val pipeline = new Pipeline().setStages(Array(assembler, rff))

  //   // train on training set
  //   val model = pipeline.fit(training_set)
  //   // evaluate on test set and print
  //   val predictions = model.transform(test_set)
  //   val rmse = rmse_evaluator.evaluate(predictions);
  //   // change the text here
  //   println(s"linear regression rmse: ${rmse}")

  // }

  
//old version used of the prcoessing 311 data please refer to the version2 
  def process_311_version1(file_path: String) = {
    //String RDD of raw clean data
    val data_raw_311 = sc.textFile(file_path)

    //splits the rdd by ","
    val date_splitted_311 =
      data_raw_311.map(row => row.substring(1, row.length - 1).split(','))

    //selects the useful data of the data which is the date,zipcode, and also the complaint type 
    val data_selected_311 =
      date_splitted_311.map(row => (row(1), row(3), row(4)))


    val data_date_formatted = data_selected_311.map(row =>
      (lag_date(format_date(row._1)), row._2, row._3)
    )

    // get top compaint types
    // val complaints_count = data_date_formatted.map(row => (row._2, 1)).reduceByKey((v1, v2) => v1 + v2).sortBy(row => row._2).collect
    // complaints_count.foreach(println)

    /*
      top 10 complaint type
      (GENERAL CONSTRUCTION,498752)
      (Street Light Condition,537034)
      (Water System,638585)
      (PLUMBING,713745)
      (HEATING,875961)
      (Street Condition,921832)
      (Blocked Driveway,948721)
      (Illegal Parking,952055)
      (HEAT/HOT WATER,1291413)
      (Noise - Residential,1867106)
     */

     //concatenates the data and zipcode to a particular format and one hot encodes the complaint types 
    val data_complaint_transformed = data_date_formatted.map(row =>
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

    // create dataframe from 311 data
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






  // this fucntion loads in the cleaned 311 data, sum the count of the top 10 ranked type of complaints by month and zipcode, and lag the date by 2 months
  // the return value is a dataframe whose key is date|zipcode, and other columns are counts of complaints of different types
  def process_311_version2(file_path: String) = 
  {
    val data_raw_311 = sc.textFile(file_path)
    val date_splitted_311 =
      data_raw_311.map(row => row.substring(1, row.length - 1).split(','))
    val data_selected_311 =
      date_splitted_311.map(row => (row(1), row(3), row(4)))
    val data_date_formatted = data_selected_311.map(row =>
      (lag_date(lag_date(format_date(row._1))), row._2, row._3)
    )

    // get top compaint types
    // val complaints_count = data_date_formatted.map(row => (row._2, 1)).reduceByKey((v1, v2) => v1 + v2).sortBy(row => row._2).collect
    // complaints_count.foreach(println)

    /*
      top 10 complaint type
      (GENERAL CONSTRUCTION,498752)
      (Street Light Condition,537034)
      (Water System,638585)
      (PLUMBING,713745)
      (HEATING,875961)
      (Street Condition,921832)
      (Blocked Driveway,948721)
      (Illegal Parking,952055)
      (HEAT/HOT WATER,1291413)
      (Noise - Residential,1867106)
     */

    val data_complaint_transformed = data_date_formatted.map(row =>
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

    // creates a dataframe from the 311 rdd 
    val row_rdd_311 =
      data_flat_311.map(row => Row.fromSeq(row.productIterator.toSeq))
    val sqlCtx = new SQLContext(sc)
    import sqlCtx._
    val df_311 = sqlCtx.createDataFrame(row_rdd_311, schema_311)
    df_311
  }

  // this fucntion loads in the cleaned airbnb data, convert the categorical variables to dummy variables
  // the return value is a dataframe whose key is date|zipcode, and other columns are features for the prediciton model 
  def process_airbnb_version1(file_path: String) = {
    // load airbnb data
    val df_raw_airbnb = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(file_path)

    val rdd_airbnb = df_raw_airbnb.rdd

    // get all nomial values types
    // val property_type_count = rdd_airbnb.map(row => (row.getString(2), 1)).reduceByKey((v1, v2) => v1 + v2).collect
    // property_type_count.filter(row => row._2 > 1000).foreach(println)
    /*
      (Hotel,2640)
      (House,50654)
      (Apartment,499176)
      (Other,1350)
      (Guest suite,4775)
      (Condominium,19436)
      (Townhouse,20873)
      (Boutique hotel,3249)
      (Loft,17761)
      (Serviced apartment,6435)
     */

    /*because tuple can only go from 2 to 22, our output is bigger than 22 fields, 
     we had to convert it to string and reconvert it back to an Array here*/
    def fields_to_numeric(arr: Array[String]) = {
      val arr_buf = ArrayBuffer[Any]()
      arr_buf += arr(0)
      for (i <- 1 to 20) {
        arr_buf += arr(i).toInt
      }
      for (i <- 21 to 25) {
        arr_buf += arr(i).toFloat
      }
      for (i <- 26 to 27) {
        arr_buf += arr(i).toInt
      }
      arr_buf += arr(28).toFloat
      arr_buf += arr(29).toInt
      arr_buf += arr(30).toFloat
      arr_buf += arr(31).toInt
      arr_buf.toArray
    }

    /*
      Processes each row of the dataframe to be in correct format for the joining of the datasets 
    */
    val rdd_airbnb_transformed =
      rdd_airbnb.map(row =>
        Row.fromSeq(fields_to_numeric(transformAirBnb(row).split(",")))
      )

    // The final schema for the airbnb Dataframe 
    val schema_airbnb =
      StructType(
        Array(
          StructField("key", StringType, nullable = false),
          StructField("borough_is_Brooklyn", IntegerType, nullable = true),
          StructField("borough_is_Staten_Island", IntegerType, nullable = true),
          StructField("borough_is_Manhattan", IntegerType, nullable = true),
          StructField("borough_is_Bronx", IntegerType, nullable = true),
          StructField("borough_is_Queens", IntegerType, nullable = true),
          StructField("property_is_Hotel", IntegerType, nullable = true),
          StructField("property_is_House", IntegerType, nullable = true),
          StructField("property_is_Apartment", IntegerType, nullable = true),
          StructField("property_is_Guest_suite", IntegerType, nullable = true),
          StructField("property_is_Condominium", IntegerType, nullable = true),
          StructField("property_is_Townhouse", IntegerType, nullable = true),
          StructField(
            "property_is_Boutique_hotel",
            IntegerType,
            nullable = true
          ),
          StructField("property_is_Loft", IntegerType, nullable = true),
          StructField(
            "property_is_Serviced_apartment",
            IntegerType,
            nullable = true
          ),
          StructField("property_is_Other", IntegerType, nullable = true),
          StructField("room_is_private_room", IntegerType, nullable = true),
          StructField("room_is_entire_room", IntegerType, nullable = true),
          StructField("room_is_shared_room", IntegerType, nullable = true),
          StructField("room_is_hotel_room", IntegerType, nullable = true),
          StructField("accommodates", IntegerType, nullable = true),
          StructField("bathrooms", FloatType, nullable = true),
          StructField("bedrooms", FloatType, nullable = true),
          StructField("beds", FloatType, nullable = true),
          StructField("price", FloatType, nullable = true),
          StructField("cleaning_fee", FloatType, nullable = true),
          StructField("host_listings_count", IntegerType, nullable = true),
          StructField("availability_90", IntegerType, nullable = true),
          StructField("extra_people", FloatType, nullable = true),
          StructField("number_of_reviews", IntegerType, nullable = true),
          StructField("security_deposit", FloatType, nullable = true),
          StructField("minimum_nights", IntegerType, nullable = true)
        )
      )

    val sqlCtx = new SQLContext(sc)
    import sqlCtx._

    //changes the rdd back to a dataframe and drop the number of reviews 
    val df_airbnb =
      sqlCtx
        .createDataFrame(rdd_airbnb_transformed, schema_airbnb)
        .drop("number_of_reviews")
    df_airbnb
  }


  //version not used please refer to version2 
  def process_real_estate_version1(file_path: String) = {
    val rdd_real_estate = sc.textFile(file_path).map(row => row.split(','))
    val rdd_real_estate_get_key = rdd_real_estate.map(row =>
      (lag_date(row(0)) + "|" + row(1), row(2).toFloat)
    )
    val row_rdd_real_estate =
      rdd_real_estate_get_key.map(row => Row.fromSeq(row.productIterator.toSeq))

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
      sqlCtx.createDataFrame(row_rdd_real_estate, schema_real_estate)
    df_real_estate.show(1)
    df_real_estate
  }


  // this fucntion loads in the formatted real estate data, create a key by month and zip code, and lag the date by 2 months
  // the return value is a dataframe whose key is date|zipcode, real_estate_price
    def process_real_estate_version2(file_path: String) = {
    //reads in the raw clean data
    val rdd_real_estate = sc.textFile(file_path).map(row => row.split(','))

    //maps the records into a PairRDD with the key being the date|zip 
    val rdd_real_estate_get_key = rdd_real_estate.map(row =>
      (lag_date(lag_date(row(0))) + "|" + row(1), row(2).toFloat)
    )

    //changes each record to row 
    val row_rdd_real_estate =
      rdd_real_estate_get_key.map(row => Row.fromSeq(row.productIterator.toSeq))

    //Final schema for the dataframe
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
      sqlCtx.createDataFrame(row_rdd_real_estate, schema_real_estate)
    df_real_estate.show(1)
    df_real_estate
  }

  // tool for process_311_version2
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

  // tool for process_real_estate_version2 and process_311_version2
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

  // tool for process_311_version1
  // transform categorical variable to one-hot vector
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
  
  // tool for process_airbnb_version1
  // transform categorical variable to one-hot vector
  // we return a String instead of a Tuple because the fields are more than 22
  def transformAirBnb(row: org.apache.spark.sql.Row) = {
    val key = row.getString(16).split('/')(0) + "-" + row
      .getString(16)
      .split('/')(1) + "|" + row.getInt(0)
    val borough_type = row.getString(1) match {
      case "Brooklyn"      => (1, 0, 0, 0, 0)
      case "Staten Island" => (0, 1, 0, 0, 0)
      case "Manhattan"     => (0, 0, 1, 0, 0)
      case "Bronx"         => (0, 0, 0, 1, 0)
      case "Queens"        => (0, 0, 0, 0, 1)
    }
    val property_type = row.getString(2) match {
      case "Hotel"              => (1, 0, 0, 0, 0, 0, 0, 0, 0, 0)
      case "House"              => (0, 1, 0, 0, 0, 0, 0, 0, 0, 0)
      case "Apartment"          => (0, 0, 1, 0, 0, 0, 0, 0, 0, 0)
      case "Guest suite"        => (0, 0, 0, 1, 0, 0, 0, 0, 0, 0)
      case "Condominium"        => (0, 0, 0, 0, 1, 0, 0, 0, 0, 0)
      case "Townhouse"          => (0, 0, 0, 0, 0, 1, 0, 0, 0, 0)
      case "Boutique hotel"     => (0, 0, 0, 0, 0, 0, 1, 0, 0, 0)
      case "Loft"               => (0, 0, 0, 0, 0, 0, 0, 1, 0, 0)
      case "Serviced apartment" => (0, 0, 0, 0, 0, 0, 0, 0, 1, 0)
      case default              => (0, 0, 0, 0, 0, 0, 0, 0, 0, 1)
    }
    val room_type = row.getString(3) match {
      case "Private room"    => (1, 0, 0, 0)
      case "Entire home/apt" => (0, 1, 0, 0)
      case "Shared room"     => (0, 0, 1, 0)
      case "Hotel room"      => (0, 0, 0, 1)
    }

    key + "," + borough_type._1 + "," + borough_type._2 + "," + borough_type._3 + "," + borough_type._4 + "," + borough_type._5 + "," + property_type._1 + "," + property_type._2 + "," + property_type._3 + "," + property_type._4 + "," + property_type._5 + "," + property_type._6 + "," + property_type._7 + "," + property_type._8 + "," + property_type._9 + "," + property_type._10 + "," + room_type._1 + "," + room_type._2 + "," + room_type._3 + "," + room_type._4 + "," + row
      .getInt(4) + "," + row.getDouble(5) + "," + row.getDouble(6) + "," + row
      .getDouble(7) + "," + row.getDouble(8) + "," + row.getDouble(9) + "," + row
      .getInt(10) + "," + row.getInt(11) + "," + row.getDouble(12) + "," + row
      .getInt(13) + "," + row.getDouble(14) + "," + row.getInt(15)

  }

}
