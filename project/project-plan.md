# Project Plan: Safety First - The Analysis of Motor Vehicle Accidents and Population Density in New York City

## Main Question
1. Is there a relationship between population density and the number of fatalities in each New York City accident?

## Description
This project analyzes patterns and relationships between motor vehicle collisions and population distribution across New York City. Using the Motor Vehicle Collisions - Crashes dataset and the NYC Population by Community Districts dataset, the study aims to identify areas with disproportionately high collision rates relative to population density. The findings will help inform policy recommendations for traffic safety and urban planning.

## Resources
### 1st: Motor Vehicle Collisions - Crashes Data
* Metadata URL: https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95/about_data
* Data Type: CSV
* Description: This dataset contains state-level monthly unemployment insurance data, including the number of unemployed individuals filing claims. 
It provides a breakdown of unemployment trends over time, which can be used to analyze labor market fluctuations across regions.
### 2nd: NYC Population by Community Districts
* Metadata URL: https://data.cityofnewyork.us/City-Government/New-York-City-Population-By-Community-Districts/xi7c-iiu2/about_data
* Data Type: CSV
* Description: This dataset provides data on the number of borrowers with approved debt cancellations by state. 
The data covers the total number of approvals, giving a quantitative look at debt relief on a state-by-state basis. 
This information will be essential for assessing economic relief efforts across regions.
### Tools
* Python (Pandas, Matplotlib, Seaborn, Scikit-learn)
* Jupyter Notebook for workflow documentation

## Work Packages

Data Collection and Cleaning
Issue #1: Download and clean data from both sources, ensuring consistent formats for state names/identifiers.

Exploratory Data Analysis (EDA)
Issue #2: Perform an initial exploration to understand data distributions, identify any outliers, and prepare for correlation analysis.

Data Integration and Merging
Issue #3: Merge datasets based on state identifiers, creating a unified dataset for analysis.

Statistical Analysis
Issue #4: Conduct correlation and regression analyses to investigate relationships between unemployment rates and debt cancellation approvals.

Visualization
Issue #5: Generate visualizations (e.g., scatter plots, heatmaps) to illustrate any patterns or trends found in the data.

Report Findings
Issue #6: Compile a report that summarizes the analysis, including visualizations and interpretations of the results.

Conclusions and Recommendations
Issue #7: Discuss potential implications of findings and provide recommendations for policymakers.
