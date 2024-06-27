import pandas as pd
import os

class ExtractGsheets:

  def __init__(self,dataset_name,id_sheetname_dict, export_path):
    
    '''
    Extract gsheet(s) to csv. Returns a csv of concatenated spreadhsheets from the sheets id and their target sheetname.
    
    dataset_name: name of the dataset to extract.
    id_sheetname_dict: dict variable of urls to the spreadsheet(s) that make up the data and the sheet name.
    export_path: path to data_source folder for extract output.
    '''

    self.id_sheetname_dict = id_sheetname_dict
    self.dataset_name = dataset_name
    self.export_path = export_path
    

    #todo: tests
    #if not isinstance(items, dict):
    #    raise TypeError("")"

  
  def __str__(self):
    return f'Extract job for {self.dataset_name}'

  def __repr__(self):
    return f'Extract job object of class ExtractGsheets for instance: {self.dataset_name}'

  def extract_data(self):
    
    '''Takes in dictionary of {ghseets_id : sheetname}, exports datasets as csv to passed folder''' 
    
    print(f"Starting extract job for {self.dataset_name}")

    for gsheetid,sheetname in self.id_sheetname_dict.items():

      gsheets_csv_url =  f'https://docs.google.com/spreadsheets/d/{gsheetid}/export?format=csv'
      Report = pd.read_csv(gsheets_csv_url, keep_default_na = False)
      Report.to_csv(os.path.join(self.export_path, f"{self.dataset_name} {sheetname}.csv"), index = False) 
      print(f"{self.dataset_name} {sheetname}.csv imported")

    print(f"File(s) succesfully extracted to {self.export_path}")
