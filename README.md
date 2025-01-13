# Economic_Statistics_ETL
  A template for scalable ETL maintenance and development operations.


## Objective
  
  The approach solves common challenges faced when collaborating on managing a scalable ETL system:

  - Organizes the code to reduce redundancy and improve its integrity.
  - Reduce codebase complexity.
  - Transparency through specific error messages.
  - Low onboarding learning curve.
  - Coordinates multiple developers' efforts.
  - The codebase solves a type of problem once.
  - Simple, optimized for debugging modules of specific functionality at each level of processing.
  - Increases the impact of a developer hour in maintaining, scaling and improving an ETL system.  
  - Expedites ETL development and deployment by leveraging templates of previous datasets.


## Example Applications: 

    Scenario #1: 25 bug tickets vs one bug that fixes 25 datasets

### Simulated context: 

One of the agencies we follow, The Department of Revenue, adds support for a special character in their Electricity datasets suite that result in errors for 25 ETLs. This data feeds critical down-stream assets and it is paramount that downtime is minimized. The original developers of these ETLs are no longer with the team.

  #### Without the standard
  A developer would:
    - Guess by exploring to find where the original developer might have written the logic causing the bug.
    - Read each module's code and familiarize with the author's coding style to fix for one dataset.
    - Repeat this process for each dataset.  
  
  #### With the new standard
A developer would:
  - Follow the standard error message to access directly the level where special characters are processed on our codebase.
  - Update the transform module used for this suite of datasets to include support for the special character if it is found.
  - Once updated, the 25 dataset's ETLs are fixed and every other dataset importing that method is now immune to that bug. All from the change of one specific function on one specific module. 


#### Conclusion

Each developer thinks and codes differently.
The OOP module approach guarantees solving the problem once whereas a more liberal functional coding approach results in having to resolve the same issue across 25 different ETL routines according to how the original developer organized his code. 


    Scenario #2 Adding a new ETL.

### Simulated context: 

The Energy Information Agency publishes a new dataset that adds carbon emissions per kWh information and we want to add that data.

#### Without the standard
 The developer:
  1. Implements and adds her personal way of extracting data from google Sheets files.
  2. Implements her conversion function in the dataset's transform module. 

#### With the standard
The developer:
  1. Imports the high-level dataset extract module used for a similar dataset and only reconfigures the required variables for the new dataset.
  2. Imports the high-level template module used for the transform routine of a similar dataset and imports the kWh to mWh function that has only been needed for one other dataset to meet our "only in kWh" data publishing requirement.

#### Conclusion
  - With the standard, the developer didn't write new logic but imported previously proven standard logic and adapted it to cover a new application.
  - By design, the amount of code that has to be written to make a change or add a new dataset is optimized to be the minimum.
  - Any new functionality is written with future reusability as a design paradigm. 

## Technologies Used:

- Python3
- Pandas
- Object Oriented Programming
