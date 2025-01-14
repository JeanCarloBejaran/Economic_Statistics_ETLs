# Economic Statistics ETL 📊

## Objective 🎯

This project tackles common challenges in managing a scalable ETL system:

- ✅ **Organized Code**: Reduces redundancy and improves integrity.
- ⬆️ **Simplified Complexity**: Makes the codebase easier to navigate.
- 🔒 **Transparent Debugging**: Specific error messages for quicker fixes.
- 📚 **Low Learning Curve**: Onboards new developers seamlessly.
- 📚 **Collaboration Ready**: Streamlines team contributions.
- ♻️ **One Problem, One Solution**: Solves issues universally, not redundantly.
- 🔧 **Optimized Debugging**: Ensures modular debugging at each level.
- ⏳ **Increased Efficiency**: Amplifies developer impact.
- 🔄 **Reusable Templates**: Leverages previous work for expedited development.

### 🔗 Quick Links to the Code

Here are the key links to the relevant modules and routines for this project:

- **Extract Module**:  
  📂 [Extract Tools](https://github.com/JeanCarloBejaran/Economic_Statistics_ETLs/tree/main/Extract_Tools)

- **Transforms Module**:  
  📂 [Colorado Department of Revenue Agency Modules](https://github.com/JeanCarloBejaran/Economic_Statistics_ETLs/tree/main/Economic_statistics_ETLs/Colorado%20Department%20of%20Revenue/Agency_Modules)

- **Extract and Transform Routines**:  
  📂 [Sales Reports ETL Modules](https://github.com/JeanCarloBejaran/Economic_Statistics_ETLs/tree/main/Economic_statistics_ETLs/Colorado%20Department%20of%20Revenue/Sales_Reports/ETL_modules)

---

## Example Applications 📊

### Scenario #1: 25 Bug Tickets vs. One Bug Fix for 25 Datasets 

**Simulated Context:**

The Department of Revenue introduces a special character in their Electricity datasets, breaking 25 ETLs. These datasets feed critical downstream assets, requiring urgent fixes. The original developers are unavailable.

**Without the Standard:**
- ❓ Guesswork to locate the issue in the original developer's code.
- 🧐 Manual review of coding styles for each dataset.
- ♻️ Repeat the process 25 times for each dataset.

**With the Standard:**
- 🔗 Follow the error message to the exact processing level.
- ⚒️ Update the transform module for the dataset suite.
- 💡 Fix all 25 datasets and future-proof the system with one change.

**Conclusion:**

The OOP module approach ensures a single solution for universal problems, contrasting with ad-hoc fixes that require repeated effort across multiple ETLs.

### Scenario #2: Adding a New ETL 🔄

**Simulated Context:**

A new dataset from the Energy Information Agency includes carbon emissions per kWh data. You need to integrate it.

**Without the Standard:**
1. Develop custom extraction logic for Google Sheets.
2. Implement a conversion function in the transform module.

**With the Standard:**
1. Import a high-level dataset extract module and reconfigure variables.
2. Use a template transform module and a reusable kWh to mWh function.

**Conclusion:**
- ✨ Minimal new code required.
- ♻️ Future-proof functionality by design.

## Technologies Used 🚀

- 🐍 Python 3
- 📊 Pandas
- 🔧 Object-Oriented Programming
