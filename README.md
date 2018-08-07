## Predicting-Airline-Delay-using-Microsoft-R-server

In this poster we have analyzed the Airline data from 1987 to 2012. This data was downloaded from Bureau of Transportation Statistics and revolutionanalytics.com. This data consists of 148 Million rows and 46 variables.

We have used 'R' Statistical software in this project since 'R' has several limitations with respect to processing ‘Big Data’. Data flow overwhelms R as it processes data by in-memory operation.
One way  to overcome the memory limitation of R is by avoiding the memory altogether and processing the data by chunks. Microsoft R server implements these capabilities with novel High Performance Analytic functions and new file format system called XDF(External Data Format). We have implemented these functions in a local computer and also in the Azure cloud computing with a customized cluster. 


## Key Findings/ Summary of Project
1) The memory limitation of R can be overcome by using Microsoft R Server and its high performance analytic functions (Fig_xx, Fig_yy)
2) A local computer installed with MRS is sufficient to  do analysis  on up to 150 million rows of data and it can be clustered in Azure  for bigger data.
3) XDF file is efficient for processing bigdata than CSV.
4) The top five carriers are Southwest, Delta, American, US Airways and United. Lowest are Virgin America,Hawaaiin, 
5) The worst performing carriers by greater than 15 minutes delay are Pacific,JetBlue etc, Southwest has only 17% delayed flights.
6) Security and Weather delays contribute very less for the overall delays and majority is by Carrier, Late aircraft, and NAS delay.  
7) Average delay caused by Airports(NAS delay) is 30 minutes and surprisingly all five busiest airports of U.S perform better than average.
8) As expected Fridays are worst for arrival and departure delays and Saturdays are the least.
9) The delay is more between 4pm and 8pm and least between 2am-8am.


## Conclusion

Though there are many Big Data packages available in R, MRS(Microsoft R server) comes as the best option to overcome the memory problem. With the Azure cloud service, enterprise security and deployable web apps are made possible. Data analysts who are proficient in R need not learn Python to solve big data problems.
For complex analysis we can use Azure cloud to create a cluster and scale it as we need.
