# Economic_Statistics_ETL
  Example of normalized ETL development operations.


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

One of the agencies we follow, The Department of Revenue, adds support for a special character in their Electricity datasets suite that result in errors for 25 ETLs. This data that feeds critical company assets and it is paramount that down time is minimized. The original developers of these ETLs are no longer with the team.

  #### Without the standard:
  A developer would:
    - Guess exploring and find where the original developer might have written the code that now enables the bug.
    - Read each module's code to learn and modify for that instance of the problem.
    - Repeat this process for each dataset.  
  
  #### Following the standard:
A developer would:
  - Access the level where special characters are processed for our codebase.
  - Update the transform instance used in this suite of electricity datasets to include support for the special character if it is found.
  - Once updated, the 25 dataset's ETLs are fixed and every other dataset importing that method is now immune to that bug. All from the change of one specific function at one specific module. 
  
  **Edit this out** 
  Using this standardized approach, a new developer can easily locate the transform routines in the ETL modules, because it is consistent across agency folders across the repo, 
    Only has to learn things once because the layout and development process is the same across agency folders.
    Propagates this function across datasets to future-proof the other 50 datasets in this suite to automatically adjust the ETL if the new conditions are met. 


#### Conclusion:

Each developer thinks and codes differently.
The OOP module approach guarantees solving the problem once whereas a more liberal functional coding approach could result in resolving the same issue across 25 different ETL routines. 


    Scenario #2 Adding a new ETL.

The Energy Information Agency publishes a new dataset that adds carbon emissions per kWh information and we want to add that dataset. 

#### Not following the standard
 The developer:
  1. Implements and adds his way of extracting data from google sheet files.
  2. Implements her conversion function in the dataset's transform module. 

#### Following the Standard
The developer:
  1. Imports the high-level dataset extract template used for a similar dataset and replaces the variables for the new dataset.
  2. Imports the high-level template used for the transform routine of a similar dataset and imports the kWh to Mwh function that has only been needed on one other dataset to meet our "only in kWh" data publishing standard.

#### Conclusion:
  - The new code written for adding a new dataset is optimized to be the minumum.
  - New functionality is written with reusability as a design paradigm. 

## Technologies Used:

- Python3
- Pandas
- Object Oriented Programming
