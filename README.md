# Project 2

## Group: VICTORIOUS

## Project Title: Covid Analysis

Project 2 is an analysis of COVID data using Spark.

We will be making queries of the data to better
understand the effects of COVID-19 on certain 
populations and to find any trends among the data.


## To be VICTORIOUS, we have set these goals:

**To reach Minimum Viable Product:**

- A package will be published on GitHub.com/adamseelye/Project_2
- Data will be loaded into Hive
- Data then cleaned, transformed with Spark, loaded into Hive warehouse
- Create a deliverable .jar file
- 12 data queries: each displayed as a graph
- Implement Spark logging
- Use Zeppelin for data visualization/analytics (graphs)

Queries:

* Pie Graph time series
	- Confirmed Cases
	- Deaths
	- Recovered
* Line Graph Country
	- Confirmed Cases
	- Deaths
	- Recovered
* Line Graph State
	- Confirmed Cases
	- Deaths
	- Recovered
* Histogram World
	- Confirmed Cases
	- Deaths
	- Recovered

**Data Trends:**


	We expect to see a slow beginning to the pandemic followed
	by explosive growth as the pandemic started to ramp up.
	Eventually, new COVID case counts reach a plateau and then
	begin to taper off; though the never reaches zero again as
	Coronavirus is still sporadically infecting populations around
	the world. COVID deaths only ever increase but we expect 
	to see a slow start followed by rapid growth and then an
	eventual decrease in the COVID death case rate. Recovery cases
	should follow a similar trend as cases may only ever end
	in recovery or death; the number will never decrease. However,
	recovery case data may be more complicated as people recovered
	and then were reinfected with COVID a second or multiple times.

Data Source:
>	**https://health.google.com/covid-19/open-data/raw-data**

Stretch Goals:

	- 3D graphs
	- complex data modeling
	- s3 bucketing
	- clean ui
	- pdf export
	- docker container
