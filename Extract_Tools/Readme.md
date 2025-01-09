# ExtractGsheets Module

## 📄 Overview

The `ExtractGsheets` module is designed to automate the extraction of data from Google Sheets. This module leverages powerful Python libraries to seamlessly convert multiple Google Sheets into CSV files, facilitating efficient data integration into your data pipelines and analytical workflows.

### What It Does

- **Data Extraction**: Connects to specified Google Sheets using their unique IDs and extracts data from designated sheets.
- **Data Conversion**: Transforms the extracted data into CSV format, ensuring compatibility with various data processing tools.
- **Data Organization**: Saves the resulting CSV files into a specified export path, organized by dataset and sheet names for easy access and management.

### Technologies Used

- **Python** 
- **Pandas**: Utilized for data manipulation and conversion from Google Sheets to CSV.
- **os Module**: Handles file path operations and interactions with the file system.

## 🚀 Features

- **Easy Extraction**: Convert multiple Google Sheets into CSV files effortlessly.
- **Flexible Configuration**: Specify multiple sheet IDs and corresponding sheet names.
- **Automated Workflow**: Automate the extraction process with simple Python code.
- **Error Handling**: Basic error handling to ensure robust data extraction.

## 📚 Usage

### Importing the Module

```python
from extract_gsheets import ExtractGsheets
