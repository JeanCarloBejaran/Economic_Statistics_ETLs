# 📊 Revenue Dataset Module

Welcome to the **Revenue Dataset Module** folder. This directory contains the `RevenueDataset.py` script, a comprehensive tool designed to transform and process datasets from a specific provider. 🚀

---

## 🛠️ Overview

The `RevenueDataset` class is the cornerstone of this module, offering functionalities that:

- Simplify data transformation workflows. 🔄
- Provide utility methods for cleaning, grooming, and exporting datasets. 🗂️
- Ensure seamless handling of provider-specific schemas and data quirks. ⚡

---

## 🗂️ Key Components

### **`RevenueDataset` Class**

The `RevenueDataset` class includes the following key attributes and methods:

- **Attributes:**
  - `dataset_name`: Name of the dataset being processed.
  - `source_files`: List of source file names to process.
  - `source_path`: Path to the folder containing the source files.
  - `output_path`: Path to save the transformed data.
  - `column_names`: Target column names for the dataset.
  - `expected_column_names`: Expected column names for validation.
  - `ignore_columns`: Columns to exclude during processing.

- **Methods:**
  - **Data Grooming:**
    - `find_header_row()`: Locates the header row in a dataset.
    - `find_footer_rows()`: Identifies footer rows in a dataset.
    - `find_money_columns()`: Detects columns containing monetary values.
  - **Data Transformation:**
    - `build_full_report()`: Aggregates, cleans, and prepares the dataset.
    - `remove_money_signs()`: Removes `$` signs from monetary values.
    - `remove_commas()`: Strips commas from numeric values.
    - `replace_NANR()`: Replaces `NA` and `NR` with blanks or moves them to a separate column.
    - `parentheses_to_negatives()`: Converts parentheses-enclosed values to negatives.
    - `replace_nan()`: Replaces `nan` values with blank spaces.
  - **Export:**
    - `export()`: Saves the transformed dataset to a CSV file.
  - **Testing:**
    - `test_extracted_data()`: Validates source files against expected schemas.

---

## 🚀 Usage

Here's a quick example of how to use the `RevenueDataset` class:

```python
from RevenueDataset import RevenueDataset

# Initialize the dataset object
dataset = RevenueDataset(
    dataset_name="ExampleDataset",
    source_files=["source_file_1.csv", "source_file_2.csv"],
    source_path="data_source/",
    output_path="data_transformed/",
    column_names=["Column1", "Column2", "Column3"],
    expected_column_names=[["Column1", "Column2", "Column3"]],
    ignore_columns=["IgnoreColumn"]
)

# Build and export the transformed dataset
dataset.build_full_report()
dataset.export()
