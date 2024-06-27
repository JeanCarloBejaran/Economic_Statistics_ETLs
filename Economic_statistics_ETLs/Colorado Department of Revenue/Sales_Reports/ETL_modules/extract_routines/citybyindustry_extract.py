import os,sys
ETL_home = os.getenv('ETL_home')
sys.path.insert(0, os.path.join(ETL_home, 'Extract_Tools')) 
import Extract_Tools

#Instance initialization variables
dataset_name = 'Sales Reports by Industry and City in Colorado'

export_path = os.path.join(ETL_home, 'Source_Agency_Folder', 'Agency_Modules', 'data_raw')

id_sheetname_dict = {'1ZKc0olDlChHyRiLlxL3ECxKqBYtyaUaB':'2016 - 2021 Report', #sheetsid / sheetname pair
                            '1E6VqKiPnUD3LMnrSBy1aEYSc4QsU6Y9v':'City by Industry'} 

#initiate instance
Report =  Extract_Tools.ExtractGsheets(dataset_name = dataset_name,    
                                       id_sheetname_dict = id_sheetname_dict, 
                                       export_path = export_path)

Report.extract_data()