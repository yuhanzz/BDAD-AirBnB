// Since directly reading xls file into rdd only brings garbled text, we firstly convert the xls files to csv files

// spark2-shell --packages com.crealytics:spark-excel_2.11:0.9.0 --executor-memory 10G --driver-memory 10G

import org.apache.spark.sql.Row

// for getting a schema for the empty dataframe
val temp_df = spark.read.format("com.crealytics.spark.excel").option("useHeader", "false").load("rdba_project/real_estate_original_data/real_estate_statenisland_year_2017.xls");

val schema = temp_df.schema;

var full_data = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema);

val district_list = List("manhattan", "bronx", "brooklyn", "queens", "statenisland");

// year : [2003 - 2019]
for (year <- 2003 to 2019) {
	for (i <- 0 to 4) {
		val file_name = "rdba_project/real_estate_original_data/real_estate_" + district_list(i) + "_year_" + year + ".xls";
		val data = spark.read.format("com.crealytics.spark.excel").option("useHeader", "false").load(file_name)
		// remove empty rows brought by the xls format prblem
		val data_removed_null = data.filter(row => row.getString(0) != null)
		// remove header rows
		val data_removed_header = data_removed_null.filter(row => row.getString(0).matches("""\d+"""))

		data_removed_header.write.format("csv").option("sep", ";").save("rdba_project/real_estate_preprocessed_data/" + "real_estate_" + district_list(i) + "_year_" + year + ".csv")
	}
}

// year : 2020
for (i <- 0 to 4) {
	val file_name = "rdba_project/real_estate_original_data/real_estate_" + district_list(i) + "_rolling.xls";
	val data = spark.read.format("com.crealytics.spark.excel").option("useHeader", "false").load(file_name)
	// remove empty rows brought by the xls format prblem
	val data_removed_null = data.filter(row => row.getString(0) != null)
	// remove header rows
	val data_removed_header = data_removed_null.filter(row => row.getString(0).matches("""\d+"""))
	val data_removed_2019 = data_removed_header.filter(row => row.getString(20).matches(".*/20"))

	data_removed_2019.write.format("csv").option("sep", ";").save("rdba_project/real_estate_preprocessed_data/" + "real_estate_" + district_list(i) + "_year_2020.csv")
}
