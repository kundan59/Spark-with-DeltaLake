The project consists of 2 java files:
1. SparkNotAcidCompliant - SparkNotAcidCompliant is a class to illustrate spark is not ACID compliant.

2. AcidTransactionWithDeltaLake - AcidTransactionWithDeltaLake is a class to illustrate how spark offer ACID Transaction with delta Lake.

## Table of contents  
1. [Getting Started](#Getting-Started) 

2. [How to Use](#How-to-Use)  
   
## Getting Started  
#### Minimum requirements  
To run the SDK you will need  **Java 1.8+, Scala 2.11.8 **.   Also you need to install spark** as prerequisites
  
#### Installation  
The way to use this project is to clone it from github and build it using maven.

## How to Use
You need to instantiate SparkNotAcidCompliant and AcidTransactionWithDeltaLake class in your project and see the resulting files in the folder sparkdata/deltalakedata to understand how delta lake provides ACID compliance to Spark.
