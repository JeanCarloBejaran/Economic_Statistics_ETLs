import os,sys
ETL_home = os.getenv('ETL_home') 
sys.path.insert(0, os.path.join(ETL_home, 'Extract_Tools')) 
import Extract_Tools


#Instance initialization variables
dataset_name = 'State Sales Tax Return History in Colorado'

export_path = os.path.join(ETL_home, 'Colorado Department of Revenue', 'Agency_Modules', 'data_raw')

id_sheetname_dict = {'1tI6ewOiPTLHWUDtdbH14Hg4NVrVI1nHx':'State'} #sheetsid / sheetname pair

#initiate instance
Report =  Extract_Tools.ExtractGsheets(dataset_name, id_sheetname_dict, export_path)

Report.extract_data()