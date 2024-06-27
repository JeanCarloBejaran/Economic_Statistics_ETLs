import os, sys
ETL_home = os.getenv('ETL_home')
sys.path.insert(0, os.path.join(ETL_home, 'Colorado Department of Revenue','Agency_Modules'))
import RevenueDataset


#Program variables
dataset_name = 'State Sales Tax Return History in Colorado' 

#paths
source_path = os.path.join(ETL_home, 'Colorado Department of Revenue', 'Sales_Reports', 'data_raw')
output_path = os.path.join(ETL_home, 'Colorado Department of Revenue', 'Sales_Reports', 'data_processed')

#Extracted file's names
source_files = ['State Sales Tax Return History in Colorado State.csv']

#Target column names
column_names = ['Month', 'Year', 'Number of Retailers ¹', 'Number of Returns ²', 'Gross Sales',	'Retail Sales',	'State Taxable Sales',	'State Sales Tax ³'] 

expected_column_names = [['Month','Year','Number of Retailers ¹','Number of Returns ²','Gross Sales','Retail Sales', 'State Taxable Sales',	'State Sales Tax ³']] #As on extracted file

#initializing instance
Report = RevenueDataset.RevenueDataset(dataset_name, 
                                       source_files, 
                                       source_path, 
                                       output_path, 
                                       column_names, 
                                       expected_column_names)

Report.test_extracted_data(column_names = True)

Report.build_full_report() 

money_columns = Report.column_names[4:] #columns after (col 2) have $ signs
Report.remove_money_signs(money_columns = money_columns)

comma_columns = Report.column_names[2:] #columns after (col 2) may have commas
Report.remove_commas(comma_columns = comma_columns)

Report.replace_NANR(money_columns= money_columns)

Report.parentheses_to_negatives(money_columns = money_columns)

Report.replace_nan()

Report.export()