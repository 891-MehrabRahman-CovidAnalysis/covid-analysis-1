# The Economic Response Group
A multi application project built to utilize Spark SQL and Amazon Elastic MapReduce to leverage cluster computing to run analytics on the regional responses to Covid-19 throughout the world. 

- - - -

>## Table of contents
>* [Description](#description)
>* [Main Feature](#feature)
>* [Screen Grab](#screen)
>* [Tech Used](#tech)
>* [Usage](#usage)
>* [Project Info](#project)
>* [Issues](#known-issues)



## Description
The Economic Response Group is one of the teams working on a larger Covid-19 project that is responsible for the building applications that ran analytics to answer questions pertaining to the global, economic impacts of the pandemic on various regions around the world. 

This application uses Spark SQL and AWS EMRto query Covid and economic dataset to answer the following questions:

* Is there a significant relationship between a Region’s cumulative GDP and Infection Rate per capita?
* What is the average amount of time it took for each region to reach its first peak in infection rate per capita?
* Which Regions handled COVID-19 the best assuming our metrics are change in GDP by percentage and COVID-19 infection rate per capita. 
* What are the top 5 landlocked countries with the highest infection rates? 

## Main Feature:
To simplify development and maintenance we used a single build.sbt file for all applications. 
Allows for centralized configuration, dependency and build management.     

Also allows us to aggregate a utilities app Into each other application to share Its resources. 

## Screen:
![Screen Shot 2021-02-19 at 2 47 51 PM](https://user-images.githubusercontent.com/48693333/108554085-98a2e900-72c1-11eb-8fe3-a4252ec6c9a0.png)

## Tech Used and Required
+ Scala 
+ sbt & sbt Assembly 
+ Spark & Spark SQL         
+ Amazon s3 and EMR              
+ Amazon SDK and CLI              
+ ScalaTest   
+ Mr. Powers (testing framework)

## Usage
These datasets can be acquired from from other references within this project and should be well documented within.                 
Contact Page at page.tyler@revature.net if there are any questions.          
<details>
    <summary><b><i>View files needed in your s3 data lake:</i></b></summary>
    <li>countries_general_stats.tsv</li>   
    <li>owid-covid-data.csv</li>    
    <li>region_dictionary (<i>rqd for any app that uses the DataFrameBuilder utility class</i>)</li>      
</details>           

These applications wil search environmental variables.    
So please bring your own AWS credentials. 

## Project:
[Main Project Repo](https://github.com/891-MehrabRahman-CovidAnalysis/covid-analysis-1)    
[Contact](https://github.com/drthisguy)    
*Email*: page.c.tyler@gmail.com       

## Known Issues:
None known at the moment.  
If any are discovered, please feel free to contact me.  Cheers. :smile:
