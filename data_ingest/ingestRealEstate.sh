rm -r /home/yz6238/rdba_project_data
mkdir /home/yz6238/rdba_project_data

dir="rdba_project/real_estate_original_data"

if $(hdfs dfs -test -d $dir) ;
then
hdfs dfs -rm -r $dir
hdfs dfs -mkdir $dir;
else
hdfs dfs -mkdir $dir;
fi


districts=(
    manhattan
    bronx
    brooklyn
    queens
    statenisland
)

dist_short=(
    manhattan
    bronx
    brooklyn
    queens
	si
)

year_half=(
	03
	04
	05
	06
)


# 2003 - 2006
for (( i=0; i<5; i++ ));
do
	for year in "${year_half[@]}"; 
	do
		wget -O ~/rdba_project_data/real_estate_${districts[i]}_year_20${year}.xls https://www1.nyc.gov/assets/finance/downloads/sales_${dist_short[i]}_${year}.xls;
		hdfs dfs -put ~/rdba_project_data/real_estate_${districts[i]}_year_20${year}.xls rdba_project/real_estate_original_data		
	done
done


# 2007 - 2009
for district in "${districts[@]}"; 
do
		# 2007
		wget -O ~/rdba_project_data/real_estate_${district}_year_2007.xls https://www1.nyc.gov/assets/finance/downloads/excel/rolling_sales/sales_2007_${district}.xls;
		hdfs dfs -put ~/rdba_project_data/real_estate_${district}_year_2007.xls rdba_project/real_estate_original_data

		# 2008
		wget -O ~/rdba_project_data/real_estate_${district}_year_2008.xls https://www1.nyc.gov/assets/finance/downloads/pdf/09pdf/rolling_sales/sales_2008_${district}.xls;
		hdfs dfs -put ~/rdba_project_data/real_estate_${district}_year_2008.xls rdba_project/real_estate_original_data

		# 2009
		wget -O ~/rdba_project_data/real_estate_${district}_year_2009.xls https://www1.nyc.gov/assets/finance/downloads/pdf/rolling_sales/annualized-sales/2009_${district}.xls;
		hdfs dfs -put ~/rdba_project_data/real_estate_${district}_year_2009.xls rdba_project/real_estate_original_data

done


# 2010 - 2020
for district in "${districts[@]}"; 
do

	for year in {2010..2017}; 
	do
		wget -O ~/rdba_project_data/real_estate_${district}_year_${year}.xls https://www1.nyc.gov/assets/finance/downloads/pdf/rolling_sales/annualized-sales/${year}/${year}_${district}.xls;
		hdfs dfs -put ~/rdba_project_data/real_estate_${district}_year_${year}.xls rdba_project/real_estate_original_data		
	done

	for year in {2018..2019}; 
	do
		wget -O ~/rdba_project_data/real_estate_${district}_year_${year}.xls https://www1.nyc.gov/assets/finance/downloads/pdf/rolling_sales/annualized-sales/${year}/${year}_${district}.xlsx;
		hdfs dfs -put ~/rdba_project_data/real_estate_${district}_year_${year}.xls rdba_project/real_estate_original_data		
	done

	# rolling sales data (2019 - 2020)
	wget -O ~/rdba_project_data/real_estate_${district}_rolling.xls https://www1.nyc.gov/assets/finance/downloads/pdf/rolling_sales/rollingsales_${district}.xls;
	hdfs dfs -put ~/rdba_project_data/real_estate_${district}_rolling.xls rdba_project/real_estate_original_data
done

