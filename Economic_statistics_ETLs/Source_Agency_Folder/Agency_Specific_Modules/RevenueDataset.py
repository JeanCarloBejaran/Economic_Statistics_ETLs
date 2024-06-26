import os
import pandas as pd

class RevenueDataset:
  def __init__(self, dataset_name, source_files, source_path, output_path, column_names, expected_column_names, ignore_columns=[]):

    '''
    source_files : Name of the files to extract on data_source folder.
    source_path: Path to the data_source folder.
    output_path: Path to the data_transformed folder.
    column_names: Names of the target columns in the extract file in a list format.
    expected_column_names: Lists of lists with the expected column names for each source file.
    ignore_columns: List of strings with column names to ignore when reading source data.
    '''
    self.dataset_name = dataset_name
    self.source_files = source_files
    self.source_path = source_path
    self.output_path = output_path
    self.column_names = column_names
    self.expected_column_names = expected_column_names
    self.ignore_columns = ignore_columns 
    
    self.dataset = pd.DataFrame(columns= self.column_names)
    

  def __str__(self):
    return f'Transform job object of class RevenueDataset for instance: {self.dataset_name}'

  def __repr__(self):
    return f'Transform job object of class RevenueDataset for instance: {self.dataset_name}'
  
  def find_header_row(self, df):
    
    '''Finds the header row of a dataframe. Returns the row number to feed the header option on pandas.read_csv().
          Method: Iterates through dataframe from top to bottom until the first row with no '' values are found. '''
    
    header_row = 0
    counter = 0

    for index,row in df.iterrows():
      blank_count = list(row.values).count('')
      if blank_count/len(list(row.values)) >= 5/16: #5/16 is the emtpy/row-len ratio in the Marijuana tax and Fee Revenue dataset. 
        counter +=1
        continue
      else:
        header_row = counter
        break

    return header_row #header row from 0 index. Note: need to add +1 to account for 0 index when using pandas.read_csv() header option.

  def find_footer_rows(self, df):
    
    '''Finds the footer rows of a dataframe. Returns the row number to feed the skipfooter option on pandas.read_csv().
        Method: Iterates through the dataframe from bottom to top. #If the row has headers-1 empty values, it's a footer row.'''
    
    iloc_header_row = self.find_header_row(df) + 1 #+1 to account for 0 index.
    footer_rows = 0
    counter = 0

    for index, row in df[::-1].iterrows():
      if list(row.values).count('') >= len(df.iloc[iloc_header_row]) -1: # -1 to account for the footer info cell in a footer row.
        counter += 1
        continue
      else:
        footer_rows = counter
        break
    return footer_rows

  def find_money_columns(self, df): #todo-tweak.
    '''Finds the columns with money values. Returns a list of column names to feed the usecols option on pandas.read_csv().
        Method: Iterates through the first row of the dataframe. If the cell value has a $ sign, it's a money column.'''
    
    money_columns = []
    for column in df.iloc[0]: #substitute df.iloc[0] with find_column names or df.iloc[find_header_row]. test what works.
      if '$' in column:
        money_columns.append(column)
    return money_columns

  def build_full_report(self):
    ''' Concats full report from source file(s), drops specified ignored columns,  assigns correct column names in preparation for transform function'''
    
    for report in self.source_files:
      source_file_path = os.path.join(self.source_path, report) #Partial dataset class instance would need this var. partialdataset(source_file_path, supercolumn_names, superignore_columns)    
      #could refactor into a child class PartialDataset(RevenueDataset) if self.source_files > 1. It would groom(adds pad, removes header/footer), store header, footer rows info, and returns a dataframe. Would avoid storing the ungroomed dataset in memory.
      
      usecols = lambda x: x not in self.ignore_columns
      df = pd.read_csv(source_file_path, usecols = usecols, keep_default_na = False)
      df = df.iloc[:, :len(self.column_names)] #Ignore any empty columns following the last expected.
            
      #grooming off header and footer rows
      header = self.find_header_row(df) +1  #+1 to account for 0 index. 
      footer_rows = self.find_footer_rows(df)
      groomed_df = pd.read_csv(source_file_path, usecols = usecols, keep_default_na = False, header = header, skipfooter= footer_rows, engine='python')
      
      try:
        groomed_df.columns = self.column_names
      except: #Needs padding. In refactor, this should be a separate method. self.pad_columns(groomed_df)
        if len(groomed_df.columns) < len(self.column_names):
          difference = len(self.column_names) - len(groomed_df.columns)
          for column in range(difference):
            groomed_df[f'Padding_{column}'] = ''
          groomed_df.columns = self.column_names

          print(f"Padded {report} with {difference} empty columns to match the target schema.")
          
        
      self.dataset = pd.concat([self.dataset,groomed_df])  # type: ignore
    
    print(f'{self.dataset_name} created from {self.source_files}.')

  def remove_money_signs(self, money_columns):

    ''' Removes '$' from strings depicting money. Money sign needs to be removed for datasync's automated capabilities.
        money_columns = list of strings with the column names with money signs to remove.    
    '''
    if not isinstance(money_columns, list): #assuring money_columns is a list.
        money_columns = [money_columns]

    for column in money_columns:
      self.dataset[column] = self.dataset[column].apply(lambda x: str(x).replace('$',''))
    
    print(f"$ Money sign removed from the values of columns: {money_columns}")

  def remove_commas(self, comma_columns):
    ''' Removes ',' from strings with int information. Commas need to be removed for datasync's automated capabilities.
        comma_columns = list of strings with the names for columns that contain commas.    
    '''
    if not isinstance(comma_columns, list): #assuring money_columns is a list.
        comma_columns = [comma_columns]
  
    for column in comma_columns:
      self.dataset[column] = self.dataset[column].apply(lambda x: str(x).replace(',',''))

    print(f"Removed commas from the values of columns: {comma_columns}")

  def replace_NANR(self, money_columns, into_separate_column = False):
     
    '''
    Target: Retail Reports and Revenue Marijuana datasets.
    Replaces NA and NR string values with empty values to make the column all ints for socrata data viz purposes.
    separate_columm = If True, places the non-int values in a separate column following the original column. Default is False.'''
    
    if not isinstance(money_columns, list): #assuring money_columns is a list.
        money_columns = [money_columns]

    #Put NA/NR values in a separate column.
    if into_separate_column: 
      df = self.dataset
      for column in money_columns:
        #create a series of the non-numeric values.
        new_column = self.dataset[column].apply(lambda x: x if x == 'NR' or x == 'NA' else '')
        insert_index = df.columns.get_loc(column) + 1                
        #insert into the dataframe at index + 1                       
        df.insert(insert_index, f'{column} Blank Code', new_column)  
      self.dataset = df
      print(f"Non-numeric values placed in a separate column following the original column.")
    
    #Replace NA/NR values with empty values in the original column.
    for column in money_columns:
      self.dataset[column] = self.dataset[column].apply(lambda x: str(x).replace('NR','').replace('NA',''))

    print("Replaced 'NA' and 'NR' string datapoints for blanks.")

  def parentheses_to_negatives(self, money_columns):
    '''Convert negative values reported from CDOR source file in parentheses to negative values.'''
    
    if not isinstance(money_columns, list):
      money_columns = [money_columns]
    
    for column in money_columns:
      self.dataset[column] = self.dataset[column].apply(lambda x: str(x).replace('(','-').replace(')',''))
  
  def replace_nan(self):
    '''Replaces nulls in dataframe. 'nan' values to ''.'''
    self.dataset.replace('nan', '', inplace = True)

    print("Replaced null values with blank spaces.")
  
  def export(self):
    
    self.dataset.to_csv(os.path.join(self.output_path,f"{self.dataset_name} ready for load.csv"), index = False)
    
    print(f"Transform Success, {self.dataset_name} ready for load.csv exported to {self.output_path}")


  #Tests
  def test_extracted_data(self, column_names = False):
    '''Functions to test the source datasets that make up a report. 
    If column_names = True, tests if the dataset has the expected columns in the expected order.'''
    
    counter = 0
    for partial_report in self.source_files:
      source_file_path = os.path.join(self.source_path, partial_report)    
      usecols = lambda x: x not in self.ignore_columns
      df = pd.read_csv(source_file_path, usecols = usecols, keep_default_na = False)

      #re-reading with correct header
      header = self.find_header_row(df) +1  #+1 to account for 0 index. 
      df = pd.read_csv(source_file_path, usecols = usecols, keep_default_na = False, header = header, engine='python')

      if column_names:
        df.columns = [col_name.replace('\n','') for col_name in df.columns] #remove new line character from some column names.
        assert list(df.columns) == self.expected_column_names[counter], f"Error: {partial_report} does not have the expected columns. CDOR may have changed the schema. Please check manually."
        
      
      counter = counter + 1