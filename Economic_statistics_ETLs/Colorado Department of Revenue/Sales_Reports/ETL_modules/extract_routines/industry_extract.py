import os,sys
ETL_home = os.getenv('ETL_home') 
sys.path.insert(0, os.path.join(ETL_home, 'Extract_Tools')) 
import Extract_Tools


#Instance initialization variables
dataset_name = 'Sales Reports by Industry in Colorado'

export_path = os.path.join(ETL_home, 'Colorado Department of Revenue', 'Agency_Modules', 'data_raw')

id_sheetname_dict = {'1WANzZQFE57J73daXvo1xu4mITy-1DmuP':'2016 -2020 Report', #sheetsid / sheetname pair
                            '1cmTJ4ZRAjBFfevT0hyCatUXDIzIG393I':'Industry'} 

#initiate instance
Report =  Extract_Tools.ExtractGsheets(dataset_name = dataset_name, 
                                       id_sheetname_dict = id_sheetname_dict, 
                                       export_path = export_path)

Report.extract_data()