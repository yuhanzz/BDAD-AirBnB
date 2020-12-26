// spark2-shell

// parse the csv file
val raw_rdd = sc.textFile("rdba_project/real_estate_preprocessed_data/*.csv/*.csv")
val rdd_separated = raw_rdd.map(row => row.split(';'))

// selected columns
// 10 : zip code	
// 15 : gross square feet	
// 19 : sale price
// 20 : date
val rdd_selected_cols = rdd_separated.map(row => (row(10), row(15), row(19), row(20)))
// count : 1611711


val rdd_removed_null = rdd_selected_cols.filter(row => row._1 != "" && row._2 != "" && row._3 != "" && row._4 != "")
// count : 1591911

// find all badly formatted str

// format number string by firstly removing commas and dollar sign
val rdd_format_num = rdd_removed_null.map(row => (row._1.trim(), row._2.replaceAll(",", "").trim(), row._3.replaceAll("[$,]", "").trim(), row._4.trim()))
// toDouble will cause java.lang.NumberFormatException.forInputString, so we need to find those records with format problem
val rdd_bad_format = rdd_format_num.filter(row => !row._2.matches("([0-9]*[.])?[0-9]+") || !row._3.matches("([0-9]*[.])?[0-9]+"))
rdd_bad_format.take(100).foreach(println)
// bad format example : (10019,0,- 0,4/30/13)

// format the data again according to the bad format records found
val rdd_format_num2 = rdd_format_num.map(row => (row._1, row._2.replaceAll("-", "").trim(), row._3.replaceAll("-", "").trim(), row._4))

val rdd_with_num = rdd_format_num2.map(row => (row._1, row._2.toDouble, row._3.toDouble, row._4))

// remove lines whose gross square feet is less than zero
val rdd_check_square_feet = rdd_with_num.filter(row => row._2 > 0)
// count : 915949

// remove lines whose sale price is less than zero
val rdd_check_sale_price = rdd_check_square_feet.filter(row => row._3 > 0)
// count : 575503

// remove lines whose zip code is abnormal
val rdd_check_zipcode = rdd_check_sale_price.filter(row => row._1.length == 5)
// count : 575448

// change date format to year-month
def formatDate(old_date:String):String = {
	val fields = old_date.split("/")
	val month = if(fields(0).length == 2) fields(0) else "0" + fields(0)
	val year = fields(2).length match {
		case 1 => "200" + fields(2)
		case 2 => "20" + fields(2)
		case 4 => fields(2)
	}
	year + "-" + month
}

val rdd_date_formatted = rdd_check_zipcode.map(row => (row._1, row._2, row._3, formatDate(row._4)))

// calculate price per square feet, and then get average by month
val rdd_pair = rdd_date_formatted.map(row => Tuple2(row._4 + "|" + row._1, (row._3 / row._2, 1)))

val rdd_month_aggregated = rdd_pair.reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))

val rdd_month_average = rdd_month_aggregated.map(row => (row._1, row._2._1 / row._2._2))

val rdd_splitted_key = rdd_month_average.map(row => (row._1.split('|'), row._2))

val rdd_final = rdd_splitted_key.map(row => (row._1(0), row._1(1), row._2))
// count : 33620

rdd_final.map(row => row.toString().substring(1, row.toString().length - 1)).saveAsTextFile("rdba_project/real_estate_cleaned_data")



