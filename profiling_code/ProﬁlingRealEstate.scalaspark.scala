val original_data = sc.textFile("rdba_project/real_estate_preprocessed_data/*.csv/*.csv")
val cleaned_data = sc.textFile("rdba_project/real_estate_cleaned_data")

// Profiling on the original dataset
println("count before cleaning: " + original_data.count)
// result:
// count before cleaning: 1611711

// Profiling on the cleaned dataset
println("count after cleaning: " + cleaned_data.count)
// result:
// count after cleaning: 33620

/* One of the reason for the deduction of records number is that we filtered out some glitchy data
by our etl code. The data filterd out are those records whose sales price or gross square feet are
not greater than zero, those records whose zip code are illegal, and those records that have null 
values in the fields we need. Besides, we also aggregate the daily records by month, because we need 
monthly data in the following analytics part, and this aggregation greatly reduce the count of the 
records. The cleaned records are sufficient to be of use. */



// Some additional profiling on the cleaned data
val cleaned_splited_data = cleaned_data.map(row => row.split(','))

val min_date = cleaned_splited_data.map(row => row(0)).min()
val max_date = cleaned_splited_data.map(row => row(0)).max()
println("date range: " + min_date + " to " + max_date)
// result:
// date range: 2003-01 to 2020-02

val min_zipcode = cleaned_splited_data.map(row => row(1)).min()
val max_zipcode = cleaned_splited_data.map(row => row(1)).max()
println("zipcode range: " + min_zipcode + " to " + max_zipcode)
// result:
// zipcode range: 10001 to 11697

val min_price_per_feet = cleaned_splited_data.map(row => row(2)).min()
val max_price_per_feet = cleaned_splited_data.map(row => row(2)).max()
println("price per square feet range: " + min_price_per_feet + " to " + max_price_per_feet)
// result:
// price per square feet range: 0.0010758472296933835 to 999.9568736595692


