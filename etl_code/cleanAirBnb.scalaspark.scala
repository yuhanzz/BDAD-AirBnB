import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.udf

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType

val mar18 = spark.read.format("csv").option("header", "true").option("escape","\"").option("multiline","true").option("inferSchema","true").load("project/data/listings/mar18.csv")

val apr18 = spark.read.format("csv").option("header", "true").option("escape","\"").option("multiline","true").option("inferSchema","true").load("project/data/listings/apr18.csv")

val may18 = spark.read.format("csv").option("header", "true").option("escape","\"").option("multiline","true").option("inferSchema","true").load("project/data/listings/may18.csv")

val jun18 = spark.read.format("csv").option("header", "true").option("escape","\"").option("multiline","true").option("inferSchema","true").load("project/data/listings/jun18.csv")

val jul18 = spark.read.format("csv").option("header", "true").option("escape","\"").option("multiline","true").option("inferSchema","true").load("project/data/listings/jul18.csv")

val aug18 = spark.read.format("csv").option("header", "true").option("escape","\"").option("multiline","true").option("inferSchema","true").load("project/data/listings/aug18.csv")

val sep18 = spark.read.format("csv").option("header", "true").option("escape","\"").option("multiline","true").option("inferSchema","true").load("project/data/listings/sep18.csv")

val oct18 = spark.read.format("csv").option("header", "true").option("escape","\"").option("multiline","true").option("inferSchema","true").load("project/data/listings/oct18.csv")

val nov18 = spark.read.format("csv").option("header", "true").option("escape","\"").option("multiline","true").option("inferSchema","true").load("project/data/listings/nov18.csv")

val dec18 = spark.read.format("csv").option("header", "true").option("escape","\"").option("multiline","true").option("inferSchema","true").load("project/data/listings/dec18.csv")

val jan19 = spark.read.format("csv").option("header", "true").option("escape","\"").option("multiline","true").option("inferSchema","true").load("project/data/listings/jan19.csv")

val feb19 = spark.read.format("csv").option("header", "true").option("escape","\"").option("multiline","true").option("inferSchema","true").load("project/data/listings/feb19.csv")

val mar19 = spark.read.format("csv").option("header", "true").option("escape","\"").option("multiline","true").option("inferSchema","true").load("project/data/listings/mar19.csv")

val apr = spark.read.format("csv").option("header", "true").option("escape","\"").option("multiline","true").option("inferSchema","true").load("project/data/listings/apr.csv")

val may = spark.read.format("csv").option("header", "true").option("escape","\"").option("multiline","true").option("inferSchema","true").load("project/data/listings/may.csv")

val jun = spark.read.format("csv").option("header", "true").option("escape","\"").option("multiline","true").option("inferSchema","true").load("project/data/listings/june.csv")

val jul = spark.read.format("csv").option("header", "true").option("escape","\"").option("multiline","true").option("inferSchema","true").load("project/data/listings/jul.csv")

val aug = spark.read.format("csv").option("header", "true").option("escape","\"").option("multiline","true").option("inferSchema","true").load("project/data/listings/aug.csv")

val sep = spark.read.format("csv").option("header", "true").option("escape","\"").option("multiline","true").option("inferSchema","true").load("project/data/listings/sep.csv")

val oct = spark.read.format("csv").option("header", "true").option("escape","\"").option("multiline","true").option("inferSchema","true").load("project/data/listings/oct.csv")

val nov = spark.read.format("csv").option("header", "true").option("escape","\"").option("multiline","true").option("inferSchema","true").load("project/data/listings/nov.csv")

val dec = spark.read.format("csv").option("header", "true").option("escape","\"").option("multiline","true").option("inferSchema","true").load("project/data/listings/dec.csv")

val jan = spark.read.format("csv").option("header", "true").option("escape","\"").option("multiline","true").option("inferSchema","true").load("project/data/listings/jan.csv")

val feb = spark.read.format("csv").option("header", "true").option("escape","\"").option("multiline","true").option("inferSchema","true").load("project/data/listings/feb.csv")

val mar20 = spark.read.format("csv").option("header", "true").option("escape","\"").option("multiline","true").option("inferSchema","true").load("project/data/listings/mar20.csv")


// val combined_uncleaned = mar19.union(apr).union(may).union(jun).union(jul).union(aug).union(sep).union(oct).union(nov).union(dec).union(jan).union(feb).union(mar20).union(mar18).union(apr18).union(may18).union(jun18).union(jul18).union(aug18).union(sep18).union(oct18).union(nov18).union(dec18).union(jan19).union(feb19)

// combined_uncleaned.write.parquet("project/data/listings/combined_unclean")


//val months = Array((mar19_clean,"mar19_clean"),(apr_clean,"apr_clean"),(may_clean,"may_clean"),(jun_clean,"jun_clean"),(jul_clean,"jul_clean"),(aug_clean,"aug_clean"),(sep_clean,"sep_clean"),(oct_clean,"oct_clean"),(nov_clean,"nov_clean"),(dec_clean,"dec_clean"),(jan_clean,"jan_clean"),(feb_clean,"feb_clean"),(mar20_clean,"mar20_clean"))

def remove_dollar: String => String = _.replaceAll("[$]", "")
def remove_dollar_udf = udf(remove_dollar)

def clean_rdd(df: org.apache.spark.sql.DataFrame,date:String):org.apache.spark.sql.DataFrame = {val temp = df.select("zipcode","neighbourhood_group_cleansed","property_type","room_type","accommodates","bathrooms","bedrooms","beds","price","cleaning_fee","host_listings_count","availability_90","extra_people","number_of_reviews","security_deposit","minimum_nights","review_scores_location","review_scores_rating","review_scores_accuracy");
//val temp = df.select("zipcode","neighbourhood_group_cleansed","property_type","room_type","accommodates","bathrooms","bedrooms","beds","price")
val zip = temp.withColumnRenamed("neighbourhood_group_cleansed","borough");
val dte = zip.withColumn("Date",lit(date));
val price = dte.withColumn("price", remove_dollar_udf($"price"))
val fees= price.filter(!(col("cleaning_fee").isNull || col("security_deposit").isNull || col("extra_people").isNull))
val clean_fee = fees.withColumn("cleaning_fee", remove_dollar_udf($"cleaning_fee"))
val security_deposit = clean_fee.withColumn("security_deposit",remove_dollar_udf($"security_deposit"))
val extra_people = security_deposit.withColumn("extra_people",remove_dollar_udf($"extra_people"))
extra_people
}


val mar19_clean = clean_rdd(mar19,"2019/03")
val apr_clean =  clean_rdd(apr,"2019/04")
val may_clean = clean_rdd(may,"2019/05")
val jun_clean = clean_rdd(jun,"2019/06")
val jul_clean = clean_rdd(jul,"2019/07")
val aug_clean = clean_rdd(aug, "2019/08")
val sep_clean = clean_rdd(sep, "2019/09")
val oct_clean = clean_rdd(oct, "2019/10")
val nov_clean = clean_rdd(nov, "2019/11")
val dec_clean = clean_rdd(dec, "2019/12")
val jan_clean = clean_rdd(jan, "2020/01")
val feb_clean = clean_rdd(feb, "2020/02")
val mar20_clean = clean_rdd(mar20, "2020/03")
val mar18_clean = clean_rdd(mar19,"2018/03")
val apr18_clean =  clean_rdd(apr,"2018/04")
val may18_clean = clean_rdd(may,"2018/05")
val jun18_clean = clean_rdd(jun,"2018/06")
val jul18_clean = clean_rdd(jul,"2018/07")
val aug18_clean = clean_rdd(aug, "2018/08")
val sep18_clean = clean_rdd(sep, "2018/09")
val oct18_clean = clean_rdd(oct, "2018/10")
val nov18_clean = clean_rdd(nov, "2018/11")
val dec18_clean = clean_rdd(dec, "2018/12")
val jan19_clean = clean_rdd(jan, "2019/01")
val feb19_clean = clean_rdd(feb, "2019/02")



// def saveCSV(df:org.apache.spark.sql.DataFrame,name:String):Unit={val path = "project/data/listings/" + name;
// df.write.format("csv").save(path);
// }

// val a = 0

// for (a<-0 to months.size-1){saveCSV(months(a)._1,months(a)._2)}

val combined = mar19_clean.union(apr_clean).union(may_clean).union(jun_clean).union(jul_clean).union(aug_clean).union(sep_clean).union(oct_clean).union(nov_clean).union(dec_clean).union(jan_clean).union(feb_clean).union(mar20_clean).union(mar18_clean).union(apr18_clean).union(may18_clean).union(jun18_clean).union(jul18_clean).union(aug18_clean).union(sep18_clean).union(oct18_clean).union(nov18_clean).union(dec18_clean).union(jan19_clean).union(feb19_clean)


//combined = combined.registerTempTable("combined")

val zipcoded = combined.filter("zipcode != ' '")

val zipcoded_clean = zipcoded.filter(col("zipcode").rlike("^\\d{5}$"))

val fin = zipcoded_clean.withColumn("zipcode",col("zipcode").cast(IntegerType)).withColumn("price",col("price").cast(DoubleType)).withColumn("cleaning_fee",col("cleaning_fee").cast(DoubleType)).withColumn("security_deposit",col("security_deposit").cast(DoubleType)).withColumn("extra_people",col("extra_people").cast(DoubleType)).toDF 

val b = fin.filter(!(col("bathrooms").isNull && col("bedrooms").isNull && col("beds").isNull))

val bath1 = b.withColumn("bathrooms", when($"bathrooms".isNull && $"bedrooms".isNull, $"beds").otherwise($"bathrooms"))
val bedrooms1 = bath1.withColumn("bedrooms", when($"bedrooms".isNull && $"beds".isNull,$"bathrooms").otherwise($"bedrooms"))
val beds1 = bedrooms1.withColumn("beds", when($"beds".isNull && $"bathrooms".isNull,$"bedrooms").otherwise($"beds"))

val bath = beds1.withColumn("bathrooms", when($"bathrooms".isNull , $"bedrooms").otherwise($"bathrooms"))
val bedrooms = bath.withColumn("bedrooms", when($"bedrooms".isNull, $"bathrooms").otherwise($"bedrooms"))
val beds = bedrooms.withColumn("beds",when($"beds".isNull,$"bedrooms").otherwise($"beds"))

val reviews= beds.filter(!(col("review_scores_location") < 5 || col("review_scores_accuracy") < 5 || col("review_scores_rating") < 50))

val dropNA = reviews.na.drop()

val clean = dropNA.drop("review_scores_rating", "review_scores_accuracy", "review_scores_location")

val zipcoded_clean = clean.filter(col("zipcode").rlike("^\\d{5}$"))


zipcoded_clean.write.format("csv").option("header","true").save("project/data/listings/combined_header")

zipcoded_clean.write.format("csv").option("header","true").save("project/data/listings/datasets/combined_header_updated")

zipcoded_clean.write.format("csv").save("project/data/listings/combined_noheader")











