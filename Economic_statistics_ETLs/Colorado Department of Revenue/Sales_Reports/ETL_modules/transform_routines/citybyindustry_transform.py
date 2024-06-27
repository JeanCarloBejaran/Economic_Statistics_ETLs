import os, sys
ETL_home = os.getenv('ETL_home')
sys.path.insert(0, os.path.join(ETL_home, 'Colorado Department of Revenue','Agency_Modules'))
import RevenueDataset


#Program variables
dataset_name = 'Sales Reports by Industry and City in Colorado'

#paths
source_path = os.path.join(ETL_home, 'Colorado Department of Revenue', 'Sales_Reports', 'data_raw')
output_path = os.path.join(ETL_home, 'Colorado Department of Revenue', 'Sales_Reports', 'data_processed')

#Extracted file's names
source_files = ['Sales Reports by Industry and City in Colorado 2016 - 2021 Report.csv', 'Sales Reports by Industry and City in Colorado City by Industry.csv']

#Target column names
column_names = ['Month', 'Year', 'NAICS', 'Industry', 'Arvada', 'Aurora', 'Boulder', 'Centennial', 'Colorado Springs', 'Denver', 'Fort Collins', 'Greeley','Lakewood', 'Longmont', 'Pueblo', 'Thornton', 'Westminster'] 

expected_column_names = [['Month','Year', 'NAICS Code ¹', 'Industry ¹',	'Arvada','Aurora','Boulder','Centennial','ColoradoSprings','Denver','Fort Collins','Greeley','Lakewood','Longmont','Pueblo','Thornton','Westminster'],['Month','Year','2022 NAICS Code ¹','Industry ¹','Arvada','Aurora','Boulder','Centennial','ColoradoSprings','Denver','Fort Collins','Greeley','Lakewood','Longmont','Pueblo','Thornton','Westminster']] #As in source file
ignore_columns = ['Sequence Number ¹'] 

#initializing instance
Report = RevenueDataset.RevenueDataset(dataset_name, 
                                       source_files, 
                                       source_path, 
                                       output_path, 
                                       column_names, 
                                       expected_column_names, 
                                       ignore_columns) #initializing instance

Report.test_extracted_data(column_names = True)

Report.build_full_report()

money_columns = Report.column_names[4:] #columns after Industry have $ signs 
Report.remove_money_signs(money_columns = money_columns)

comma_columns = Report.column_names[4:] #columns after Industry have commas 
Report.remove_commas(comma_columns = comma_columns)

Report.replace_NANR(money_columns= money_columns, into_separate_column = True)

Report.parentheses_to_negatives(money_columns = money_columns)

Report.replace_nan()

Report.export()