# Data and Feature Definitions

This document provides a central hub for the raw data sources, the processed/transformed data, and feature sets. More details of each dataset is provided in their respective Data Summary Reports. 

For each data, an individual report describing the data schema, the meaning of each data field, and other information that is helpful for understanding the data is provided. If the dataset is the output of processing/transforming/feature engineering existing data set(s), the names of the input data sets, and the links to scripts that are used to conduct the operation are also provided. 

For each dataset, the links to the sample datasets in the _**Data**_ directory are also provided. 


## Raw Data Sources


| Dataset Name | Original Location   | Destination Location  | Data Movement Tools / Scripts | Link to Report |
| awe_2017_20180223| \\plano\EDSA_Data-Science\nathalie_blume| Kept in Plano for best PHI governance | NA | H:\HS\_PROJECTS\Readmissions\Docs\Data_Report\RawDataSet1SummaryReport |
| Dataset NUM | Brief description of its orignal location | Brief description of its destination location | [scriptNUM.py](link/to/python/script/file/in/Code) | [Raw Dataset NUM Report](https://dsghe.lacare.org/bshelton/DataScience-ProjectTemplate/blob/master/Docs/Data_Report/RawDataSetNUMSummaryReport.md)|
| Dataset 2 | Brief description of its orignal location | Brief description of its destination location | [script2.R](link/
to/R/script/file/in/Code) | [Raw Dataset 2 Report](https://dsghe.lacare.org/bshelton/DataScience-ProjectTemplate/blob/master/Docs/Data_Report/RawDataSet2SummaryReport.md)|


* Dataset1 summary. Risk Adjustment's AWE forms from mobile visits. Located on Plano. 
* Dataset2 summary. <Provide brief summary of the data, such as how to access the data. More detailed information should be in the Dataset2 Report.> 

## Processed Data
| Processed Dataset Name | Input Dataset(s)   | Data Processing Tools/Scripts | Link to Report |
| ---:| ---: | ---: | ---: | 
| Processed Dataset 1 | [Raw Dataset 1](https://dsghe.lacare.org/bshelton/DataScience-ProjectTemplate/blob/master/Docs/Data_Report/RawDataSet1SummaryReport.md), [Raw Dataset 2](https://dsghe.lacare.org/bshelton/DataScience-ProjectTemplate/blob/master/Docs/Data_Report/RawDataSet2SummaryReport.md) | [Python_Script1.py](link/to/python/script/file/in/Code) | [Processed Dataset 1 Report](https://dsghe.lacare.org/bshelton/DataScience-ProjectTemplate/blob/master/Docs/Data_Report/ProcessedDataSet1SummaryReport.md)|
| Processed Dataset 2 | [Raw Dataset 2](https://dsghe.lacare.org/bshelton/DataScience-ProjectTemplate/blob/master/Docs/Data_Report/RawDataSet2SummaryReport.md) |[script2.R](link/to/R/script/file/in/Code) | [Processed Dataset 2 Report](https://dsghe.lacare.org/bshelton/DataScience-ProjectTemplate/blob/master/Docs/Data_Report/ProcessedDataSet2SummaryReport.md)|

* Processed Data1 summary. <Provide brief summary of the processed data, such as why you want to process data in this way. More detailed information about the processed data should be in the Processed Data1 Report.>
* Processed Data2 summary. <Provide brief summary of the processed data, such as why you want to process data in this way. More detailed information about the processed data should be in the Processed Data2 Report.> 

## Feature Sets

| Feature Set Name | Input Dataset(s)   | Feature Engineering Tools/Scripts | Link to Report |
| ---:| ---: | ---: | ---: | 
| Feature Set 1 | [Raw Dataset 1](https://dsghe.lacare.org/bshelton/DataScience-ProjectTemplate/blob/master/Docs/Data_Report/RawDataSet1SummaryReport.md), [Processed Dataset 1](https://dsghe.lacare.org/bshelton/DataScience-ProjectTemplate/blob/master/Docs/Data_Report/ProcessedDataSet1SummaryReport.md) | [R_Script2.R](link/to/R/script/file/in/Code) | [Feature Dataset1 Report](https://dsghe.lacare.org/bshelton/DataScience-ProjectTemplate/blob/master/Docs/Data_Report/FeatureDataSet1SummaryReport.md)|
| Feature Set 2 | [Processed Dataset 2](https://dsghe.lacare.org/bshelton/DataScience-ProjectTemplate/blob/master/Docs/Data_Report/ProcessedDataSet2SummaryReport.md) |[SQL_Script2.sql](link/to/sql/script/file/in/Code) | [Feature Dataset2 Report](https://dsghe.lacare.org/bshelton/DataScience-ProjectTemplate/blob/master/Docs/Data_Report/FeatureDataSet2SummaryReport.md)|

* Feature Set1 summary. <Provide detailed description of the feature set, such as the meaning of each feature. More detailed information about the feature set should be in the Feature Set1 Report.>
* Feature Set2 summary. <Provide detailed description of the feature set, such as the meaning of each feature. More detailed information about the feature set should be in the Feature Set2 Report.> 
