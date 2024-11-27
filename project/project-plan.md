# Project Plan: Safety First - The Analysis of Motor Vehicle Accidents and Population Density in New York City

## Main Question
1. Is there a relationship between population density and the number of fatalities in each New York City accident?

## Description
This project analyzes patterns and relationships between motor vehicle collisions and population distribution across New York City. Using the Motor Vehicle Collisions - Crashes dataset and the NYC Population by Community Districts dataset, the study aims to identify areas with disproportionately high collision rates relative to population density. The findings will help inform policy recommendations for traffic safety and urban planning.

## Resources
### 1st: Motor Vehicle Collisions - Crashes Data
* Metadata URL: https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95/about_data
* Data URL: https://data.cityofnewyork.us/resource/h9gi-nx95.csv
* Data Type: CSV
* Description: This dataset records NYC motor vehicle collision details, including time, location, contributing factors, and outcomes such as injuries or fatalities.
### 2nd: NYC Population by Community Districts
* Metadata URL: https://data.cityofnewyork.us/City-Government/New-York-City-Population-By-Community-Districts/xi7c-iiu2/about_data
* Data URL: https://data.cityofnewyork.us/resource/xi7c-iiu2.csv
* Data Type: CSV
* Description: The dataset provides demographic details about NYC's population, categorized by community district, including age, race, and total population.
### 3rd: NYC Roads
* Metadata URL: https://data.cityofnewyork.us/City-Government/road/svwp-sbcd
* Data URL: https://data.cityofnewyork.us/api/views/8rma-cm9c/rows.csv?accessType=DOWNLOAD
* Data Type: CSV
* Description: The dataset provides geographic details about NYC's roads, categorized by community district.
### Tools
* Python (Pandas, Matplotlib, Seaborn, Scikit-learn)
* Jupyter Notebook for workflow documentation

## Work Packages

#### 1. Data Collection and Cleaning
Download and clean data from both sources, ensuring consistent formats for state names/identifiers.

#### 2. Exploratory Data Analysis (EDA)
Perform an initial exploration to understand data distributions, identify any outliers, and prepare for correlation analysis.

#### 3. Data Integration and Merging
Merge datasets based on state identifiers, creating a unified dataset for analysis.

#### 4. Statistical Analysis
Conduct correlation and regression analyses to investigate relationships between unemployment rates and debt cancellation approvals.

#### 5. Visualization
Generate visualizations (e.g., scatter plots, heatmaps) to illustrate any patterns or trends found in the data.

#### 6. Report Findings
Compile a report that summarizes the analysis, including visualizations and interpretations of the results.

#### 7. Conclusions and Recommendations
Discuss potential implications of findings and provide recommendations for policymakers.
