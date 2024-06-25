# Economic_Statistics_ETL
  Example development template for Standardized Development operations.

## Objective
  
  The approach solves common challenges faced when collaborating on managing a scalable ETL system:

  - Reduce codebase complexity.
  - Efficient debugging through concise, standardized error messages.
  - Coordinates multiple developers collaborating on the system.
  - Lowers the learning curve for new developers to contribute. 
  - Significant increases in the impact of a developer hour in maintaining, improving, and growing an ETL system.
  - Easily deploy new ETLs by stringing together previously used logic through an extract, transform, or load template similar across a family of datasets. 
  - Easily understandable modules that separate the details of the code from the routine logic of the extract, transform, and load logic leading to simpler code reading.


## Example Applications: 

    Scenario #1: Fix a bug for 25 datasets

The Department of Revenue adds support for a special character in their Electricity Prices dataset that breaks the current logic of our ETL for 25 datasets. The original developers for these ETLs are no longer with the team.

  ####  Before the standard:
  A developer would have to read the code to learn how the original developer laid out and solved the problem. The current developer must guess explore and find where the original developer might have written the code that enabled the bug.
  
  #### Following the standard:
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
  2. Imports the high-level template used for the transform routine of a similar dataset and imports the kWh to Mwh function that has only been needed on one other dataset to meet our kWh data publishing standard.

#### Conclusion:
  The new code written for adding a new dataset will only 

## Technologies Used:

- Python3
- Pandas
- Object Oriented Programming
