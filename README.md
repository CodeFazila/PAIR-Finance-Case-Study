<h1> PAIR-Finance-Case-Study </h1>
We have been provided a data simulator. The simulator writes 3 records per second into a table in PostgresSQL called devices.

<h2> Task: Data ETL </h2>
The data generated above needs to be pulled, transformed and saved into a new database environment. Create an ETL pipeline that does the following:

* Pull the data from PostgresSQL
* Calculate the following data aggregations:<br>
a. The maximum temperatures measured for every device per hours.<br>
b. The amount of data points aggregated for every device per hours.<br>
c. Total distance of device movement for every device per hours.
* Store this aggregated data into the provided MySQL database

<h2> Screenshots </h2>
Following is the link of screenshot showing the output of task.<br>
https://github.com/CodeFazila/PAIR-Finance-Case-Study/blob/main/Final_Output.PNG

<h2> How the Project works? </h2>
When the project runs for the first time, it will fetch data of all pevious hours (if there is any) except the current hour, will perform data aggregations and then push to the MySQL database table `analytics.aggregated_data`.<br><br>
<b>Note:</b> It will then sleep until the current hour completes. When current hour reaches to the end, it will again fetch all records of last hour and dump aggregated data to target database and this program will keep performing like this.  

<h2> Alternative ways to achieve this task </h2>
We can achieve this task by following ways:
* Writing PostgreSQL query to fetch aggregated calculations.
* We can do calculations of current hour in real time instead of sleeping the program. 
