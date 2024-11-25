# Project Plan

## Title
Unemployment & Debt Cancellation Insights by State in the United States

## Main Question
1. Is there a relationship between state-level unemployment rates and the total number of student loan debt cancellations?

## Description
This project aims to explore potential correlations between state-level unemployment rates and the total number of student loan debt cancellations. 
Unemployment and student debt relief are critical indicators of economic health and individual financial stability. 
By analyzing data from both domains, this study may provide insights into how economic stress, indicated by unemployment, may correlate with federal debt relief actions in education. 
The study will use correlation analysis and visualization techniques to identify patterns, if any, across different states. 
Insights from this analysis may inform discussions on economic support policies.

## Datasources
### Datasource1: Unemployment Insurance Data
* Metadata URL: https://oui.doleta.gov/unemploy/DataDownloads.asp#ETA_203
* Data URL: https://oui.doleta.gov/unemploy/csv/ar203.csv
* Data Type: CSV
* Description: This dataset contains state-level monthly unemployment insurance data, including the number of unemployed individuals filing claims. 
It provides a breakdown of unemployment trends over time, which can be used to analyze labor market fluctuations across regions.
### Datasource1: Student Debt Cancellation Approvals by State
* Metadata URL: https://catalog.data.gov/dataset/approved-debt-relief-by-state-3224c/resource/c9899a70-0a2a-4e89-94e2-4b892225c10a
* Data URL: https://data.ed.gov/dataset/bdcca3fd-4841-41df-8b0d-cbd455748a2d/resource/667fc964-9dc6-4f71-8124-24926e18fc10/download/1024-total-number-of-borrowers-with-approved-debt-cancellation-by-state_total-approvals-by-state.csv
* Data Type: CSV
* Description: This dataset provides data on the number of borrowers with approved debt cancellations by state. 
The data covers the total number of approvals, giving a quantitative look at debt relief on a state-by-state basis. 
This information will be essential for assessing economic relief efforts across regions.

## Work Packages

Work Packages
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
This project plan outlines each step necessary to systematically analyze the correlation between unemployment and debt cancellation rates across the U.S.

## References and footnotes

[^r1]: https://oui.doleta.gov/unemploy/DataDownloads.asp#ETA_203

[^r2]: https://catalog.data.gov/dataset/approved-debt-relief-by-state-3224c
