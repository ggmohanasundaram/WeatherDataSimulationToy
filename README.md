# Weather Data Simulator

Overview
--------
  Weather Data Simulator generates weather data from the internal data store and emits weather report.
    - This simulator has capability to generate the output with real time data and offline data
    - This Simulator has capability to update the internal data store with real time data
    - This simulator is providing a option to the users to define number of output records
    - This simulator generates and emits random data every time

High-Level Architecture:
-----------------------
 1. Weather Data Simulator fetch real time  data from external source.(Data Sky)
 2. Real time data is transformed into required format and loads into Internal database as training data
 3. Emits output data


                        ----------     -------------------------------    ----------------------
                        |Dark Sky |    | Internal Training Data Store |   | Weather Data Report |
                         ----------      -----------------------------    -----------------------
                              |              ^               |                ^
                              |              |               |                |
                             \ /             |              \ /               |
                            ________________________       ___________________________
                            | TrainingData Generator|      | Weather Data Generator  |
                            ----------------------------------------------------------
                            |              Weather Data Simulator                    |
                            ---------------------------------------------------------


#### Choice of technology stack:
 1. Apache Spark Mlib - RandomForest
 2. Scala 11.8
 3. sbt - Build tool

#### Design Decisions:

         1. ETL Process-
             - This application has been designed as a ETL component
             - Extract the weather data from external sources
             - Transform the data into required format
             - Load the transformed data into Internal data store

         2. Extensibility
             - This application has been designed to support diverse input systems and output source
             - Three interfaces have been provided to achieve extensibility,
                     1. DataGenerator
                     2. Configuration Reader

             - input and output sources can be added to the simulator by inheriting those interfaces

         3. Reliability
             - The application has been designed to  emit weather report offline from internal store


#### Usage

Generate Weather Report:
-----------------------
- sbt run  
  ( default  count is 10)
        output
        -------
        0|Sydney|-33.86,151.20,17.00|2019-03-20T12:14:59|Sunny|+71.04|1013.59|0.85
        1|Wynyard|-40.98,145.72,58.00|2019-03-20T12:14:59|Sunny|+67.07|1017.46|0.82
        2|Westmead|-33.80,150.98,41.00|2019-03-20T12:14:59|Sunny|+70.94|1013.67|0.84
        3|Newcastle|-32.92,151.77,13.00|2019-03-20T12:14:59|Sunny|+72.20|1013.40|0.81
        4|PendleHill|-33.80,150.95,52.00|2019-03-20T12:14:59|Sunny|+70.40|1013.67|0.84
        5|Chatswood|-33.79,151.18,98.00|2019-03-20T12:14:59|Sunny|+68.77|1013.64|0.88
        6|Parramatta|-33.81,151.00,12.00|2019-03-20T12:14:59|Sunny|+70.94|1013.50|0.82
        7|Hornsby|-33.70,151.09,191.00|2019-03-20T12:14:59|Sunny|+68.29|1013.64|0.89
        8|Waitara|-33.70,151.10,171.00|2019-03-20T12:14:59|Sunny|+68.29|1013.64|0.89
        9|Strathfield|-33.88,151.08,38.00|2019-03-20T12:14:59|Sunny|+71.04|1013.59|0.84
        
-  sbt "run --output-count 20" 
       
       output
       -------  
       0|Sydney|-33.86,151.20,17.00|2019-03-20T12:18:10|Sunny|+71.21|1015.83|0.83
       1|Wynyard|-40.98,145.72,58.00|2019-03-20T12:18:10|Unknown|+65.72|1018.60|0.81
       2|Westmead|-33.80,150.98,41.00|2019-03-20T12:18:10|Sunny|+71.08|1015.83|0.83
       3|Newcastle|-32.92,151.77,13.00|2019-03-20T12:18:10|Sunny|+72.48|1015.72|0.81
       4|PendleHill|-33.80,150.95,52.00|2019-03-20T12:18:10|Sunny|+70.69|1015.83|0.83
       5|Chatswood|-33.79,151.18,98.00|2019-03-20T12:18:10|Sunny|+69.38|1015.91|0.88
       6|Parramatta|-33.81,151.00,12.00|2019-03-20T12:18:10|Sunny|+71.21|1015.83|0.81
       7|Hornsby|-33.70,151.09,191.00|2019-03-20T12:18:10|Sunny|+68.19|1015.80|0.90
       8|Waitara|-33.70,151.10,171.00|2019-03-20T12:18:10|Sunny|+68.19|1015.80|0.91
       9|Strathfield|-33.88,151.08,38.00|2019-03-20T12:18:10|Sunny|+71.08|1015.83|0.83
       10|Melbourne|-37.81,144.96,25.00|2019-03-20T12:18:10|Sunny|+71.00|1015.90|0.75
       11|Carlton|-37.80,144.96,45.00|2019-03-20T12:18:10|Sunny|+71.08|1015.90|0.75
       12|Docklands|-37.81,144.94,10.00|2019-03-20T12:18:10|Sunny|+71.21|1015.94|0.74
       13|Flemington|-37.78,144.92,18.00|2019-03-20T12:18:10|Sunny|+71.21|1015.90|0.75
       14|Kensington|-33.91,151.22,25.00|2019-03-20T12:18:10|Sunny|+71.00|1015.83|0.83
       15|Southbank|-37.82,144.95,4.00|2019-03-20T12:18:10|Sunny|+71.21|1015.94|0.75
       16|North Melbourne|-37.79,144.94,10.00|2019-03-20T12:18:10|Sunny|+71.21|1015.94|0.74
       17|South Wharf|-37.82,144.95,4.00|2019-03-20T12:18:10|Sunny|+71.21|1015.94|0.75
       18|South Yarra|-37.84,144.98,17.00|2019-03-20T12:18:10|Sunny|+71.21|1015.90|0.77
       19|West Melbourne|-37.80,144.92,3.00|2019-03-20T12:18:10|Sunny|+71.21|1015.94|0.74

- Generate the training data
    
    sbt "run --app-name TrainingDataGenerator"
     
    Prerequisite to run -
    		- Please create a datasky appid (https://darksky.net/dev/login)
    		- configure the appid in  /src/main/resources/conf/trainingdata.conf
    		

- To run test cases:
     sbt test

####  Internal TrainingData  Store - trainingData/
