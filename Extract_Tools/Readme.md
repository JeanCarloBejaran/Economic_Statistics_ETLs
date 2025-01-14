# 📄 ExtractGsheets Module

The `ExtractGsheets` module is designed to automate the extraction of data from Google Sheets. It leverages Python's powerful libraries to seamlessly convert multiple Google Sheets into CSV files, enabling efficient data integration into your pipelines and analytical workflows.

---

## 🎯 What It Does

1. **Data Extraction**:  
   Connects to specified Google Sheets using their unique IDs and extracts data from designated sheets.

2. **Data Conversion**:  
   Transforms the extracted data into CSV format for compatibility with various data processing tools.

3. **Data Organization**:  
   Saves the resulting CSV files into a specified export path, organized by dataset and sheet names for easy access and management.

---

## 🛠️ Technologies Used

- **Python**: The core programming language.
- **Pandas**: Utilized for data manipulation and conversion from Google Sheets to CSV.
- **os Module**: Handles file path operations and interactions with the file system.

---

## 🚀 Features

- **Easy Extraction**: Convert multiple Google Sheets into CSV files effortlessly.  
- **Flexible Configuration**: Specify multiple sheet IDs and corresponding sheet names.  
- **Automated Workflow**: Simplify the extraction process with straightforward Python code.  
- **Error Handling**: Basic error handling ensures robust data extraction.  

---

## 🛠️ Class Overview: `ExtractGsheets`

The `ExtractGsheets` class simplifies the process of extracting Google Sheets data into CSV files. Below is a summary of its attributes, methods, and usage instructions.

### 📋 Attributes

- **`dataset_name`**:  
  The name of the dataset being extracted. This will be used to name the resulting CSV files.  
  **Type**: `str`

- **`id_sheetname_dict`**:  
  A dictionary containing the Google Sheets IDs as keys and their corresponding sheet names as values.  
  **Type**: `dict`

- **`export_path`**:  
  The directory path where the extracted CSV files will be saved.  
  **Type**: `str`

---

### 🔍 Methods

#### 1. `__init__(dataset_name, id_sheetname_dict, export_path)`
Initializes an instance of the `ExtractGsheets` class.  
**Parameters**:
- `dataset_name`: The name of the dataset.
- `id_sheetname_dict`: A dictionary of `{Google Sheet ID: Sheet Name}`.
- `export_path`: Path to save the extracted CSV files.

---

#### 2. `__str__()`
Returns a string representation of the extract job.  
**Example Output**:  
`"Extract job for Sales Data"`

---

#### 3. `__repr__()`
Returns a detailed string representation of the extract job object.  
**Example Output**:  
`"Extract job object of class ExtractGsheets for instance: Sales Data"`

---

#### 4. `extract_data()`
Extracts data from the specified Google Sheets and exports them as CSV files to the target folder.  
**How It Works**:
1. Iterates through the `id_sheetname_dict` to access each Google Sheet by its ID.
2. Converts each sheet into a CSV file.
3. Saves the CSV file in the specified `export_path`.

**Output**:  
CSV files named in the format `{dataset_name} {sheetname}.csv`.

---

## 🚀 Usage Example

```python
from ExtractGsheets import ExtractGsheets

# Define the input parameters
dataset_name = "Sales Data"
id_sheetname_dict = {
    "google_sheet_id_1": "January",
    "google_sheet_id_2": "February"
}
export_path = "data_source/"

# Create an instance of the ExtractGsheets class
extract_job = ExtractGsheets(dataset_name, id_sheetname_dict, export_path)

# Perform the extraction
extract_job.extract_data()
