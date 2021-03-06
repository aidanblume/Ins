# Project Charter

## Business background

* Who is the client and what strategic business function does the client perform?

	* Dino Kasdagly, COO
	* (Interested) Katrina Miller, Chief Quality and Information Executive, Health Services
	* (Interested) Chriss Wheeler, Dir., Care Management
	* (Interested) Mark Ishimatsu, Dir. Risk Adjustment Strategies and Initiatives

* What business problem(s) are we trying to address?

	Preventable readmissions are costing money and are an adverse event. We want to reduce their rate. 

## Scope
* What data science solutions are we trying to build?

	* We are trying to build a model to assign each member a score relative to the member's risk of being readmitted within 30 days of an index discharge. 
	* Optionally, we will associate with that score a list of interventions that would reduce the score the most. 
	* We will try to produce these scores in real time during the member's index admission.
	* We may limit our population to DHS inpatients, or to patients eligible for Care Management. 
	
* What will we do?

	We will use supervised learning methods to build a series of models that predict readmission for distinct diagnoses. 
	
* How is it going to be consumed by the customer?

	We don't know yet as we are still looking for a concrete customer. 

## Personnel
* Who are on this project:
	* Data Team:
		* Data scientist(s)
			Nathalie Blume
		* Data subject matter expert(s) 
			Richard Meadows, Chriss Wheeler
		* Data engineer(s)
			EDW Team under KJ Singh in the Oracle environment. Nathalie Blume in the Cloudera environment.
		* Data administrator
			EDW Team under KJ Singh in the Oracle environment. Nathalie Blume in the Cloudera environment.
	* Project Client:
		* Business owner
			Brandon Shelton
		* Business domain subject matter expert(s)
			Chriss Wheeler
		* Project sponsor
			Dino Kasdagly
	
## Metrics
* What are the qualitative objectives? (e.g. reduce user churn)
	* Reduce readmission
	* Not increase mortality
	
* What is a quantifiable metric  (e.g. reduce the fraction of users with 4-week inactivity)
	* Reduce the fraction of members discharged from an inpatient facility who are subsequently readmitted within 30 days (I can make this more detailed in a future draft).
	* Some measure of improvement or stability in mortality? Some measure of similarity to a control group?
	
* Quantify what improvement in the values of the metrics are useful for the customer scenario (e.g. reduce the  fraction of users with 4-week inactivity by 20%) 
	* UNK
* What is the baseline (current) value of the metric? (e.g. current fraction of users with 4-week inactivity = 60%)
	* UNK
	
* How will we measure the metric? (e.g. A/B test on a specified subset for a specified period; or comparison of performance after implementation to baseline)
	* Needs to be specified.

## Plan
* Phases (milestones), timeline, short description of what we'll do in each phase.
	* See the [Readmission Road Map](./Docs/Project//Project-Artifacts/Project-Updates/Readmissions_Road_Map.vsdx) 
	* Need to list phases here. Think of what level of information will be useful. 

## Architecture
* Data
  * What data do we expect? Raw data in the customer data sources (e.g. on-prem files, SQL, on-prem Hadoop etc.)
* Data movement from source systems to Cloudera Data Hub using what data movement tools (Sqoop, Flume etc.) to move either
  * all the data, 
  * sampled data enough for modeling 

* What tools and data storage/analytics resources will be used in the solution e.g.,
  * Sqoop for RDMS ingestion
  * HDFS for storage
  * R/Python/Hive/Impala for feature construction, aggregation and sampling
  * SparkML for modeling and operationalization
* How will the score or operationalized solution be consumed in the business workflow of the customer? If applicable, write down pseudo code for the APIs of the web service calls.
  * How will the customer use the model results to make decisions
  * Data movement pipeline in production
  * Make a 1 slide diagram showing the end to end data flow and decision architecture:
  ![alt text](https://dsghe.lacare.org/bshelton/DataScience-ProjectTemplate/blob/master/Docs/Project/Project-Artifacts/data-science-template-flow.png)
  
    * If there is a substantial change in the customer's business workflow, make a before/after diagram showing the data flow.

## Communication
* How will we keep in touch? Weekly meetings?
	* Weekly 1:1 between Nathalie Blume and Brandon Shelton
	
* Who are the contact persons on both sides?
	* Data Team: Nathalie Blume, x6798, nblume@lacare.org
	* Project Client: ??
