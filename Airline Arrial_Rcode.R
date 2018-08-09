

#A compute context allows you to control whether computation will be performed locally on the edge node, or whether it will be distributed across the nodes in the HDInsight cluster.
#The Spark context will distribute the processing over all the worker nodes in the HDInsight cluster.
# Define the Spark compute context
mySparkCluster <- RxSpark(consoleOutput=TRUE)
# Set the compute context
rxSetComputeContext(mySparkCluster)
# Define the HDFS (WASB) file system
We first create a file system object that uses the default values:
  hdfsFS <- RxHdfsFileSystem()
# create a local folder for storing data temporarily bigDataDirRoot <- "/AirOnTime"
# Set directory in bigDataDirRoot to load the data into.. and specify the input file in HDFS to analyze
inputDir <- file.path(bigDataDirRoot,"AirOnTime87to12.xdf")


# We will create a data source using this file, specifying that it is on the Hadoop Distributed File System.
#Creating a data source can be useful if you have a file that should have different default settings in different analyses. They also have one other advantage: because a data source is a real R object, instead of just a file path, you can use a handful open-source R functions on them:
  
  bigAirDS <- RxXdfData( inputDir,fileSystem =hdfsFS)
# Retrieves header information from an. XDF file or summary information from an active data set 
rxGetInfo(bigAirDS,getVarInfo = TRUE)
#Estimating a Linear Model with a Huge Data Set
#The RevoScaleR compute engine is designed to work very efficiently with .xdf files, particularly with factor data. When working with larger data sets, the blocksPerRead argument is important in controlling how much data is processed in memory at one time
delayArr <- rxLinMod(ArrDelay ~ DayOfWeek, data = bigAirDS,
                     cube = TRUE, blocksPerRead = 30)

#we can compare Arrival Delays with Departure Delays. First, we rerun the linear model, this time using Departure Delay as the dependent variable.

delayDep <- rxLinMod(DepDelay ~ DayOfWeek, data = bigAirDS,
                     cube = TRUE, blocksPerRead = 30)

cubeResults <- rxResultsDF(delayArr)
cubeResults$DepDelay <- rxResultsDF(delayDep)$DepDelay



#plot the results, comparing the Arrival and Departure Delays by the Day of Week
rxLinePlot( ArrDelay + DepDelay ~ DayOfWeek, data = cubeResults,
            title = 'Average Arrival and Departure Delay by Day of Week',
            yTitle="Arrival Delay & Departure Delay",
            lineColor = c('darkcyan','blueviolet'),
            lineWidth = 3,plotAreaColor ="White")

arrDelayLm3 <- rxLinMod(ArrDelay ~ DayOfWeek:F(CRSDepTime),
                        data = bigAirDS, cube = TRUE)

arrDelayDT <- rxResultsDF(arrDelayLm3)

#plot the results, to examine the Arrival Delay by the Day of Week and departure hour.
rxLinePlot( ArrDelay~CRSDepTime|DayOfWeek, data = arrDelayDT,
            title = "Average Arrival Delay by Day of Week by Departure Hour",
            xTitle="Scheduled Departure Time",yTitle="Arrival Delay",
            lineColor = 'green',
            lineWidth = 2,plotAreaColor ="White")

delayArr <- rxLinMod(ArrDelay ~ UniqueCarrier, data = bigAirDS,
                     cube = TRUE, blocksPerRead = 30)

delayDep <- rxLinMod(DepDelay ~ UniqueCarrier, data = bigAirDS,
                     cube = TRUE, blocksPerRead = 30)

cubeResults <- rxResultsDF(delayArr)
cubeResults$DepDelay <- rxResultsDF(delayDep)$DepDelay



rxLinePlot( ArrDelay + DepDelay ~ UniqueCarrier, data = cubeResults,
            title = 'Average Arrival and Departure Delay by Carrier',
            yTitle="Arrival Delay & Departure Delay",
            lineColor = c('seagreen','mediumorchid3'),
            lineWidth = 3,plotAreaColor ="White")


# Running the summary for ArrDelay variable
rxSummary(~ArrDelay,data=bigAirDS)

countcarrier <- read.csv(file.choose())
countcarrier

## Re-level the counts by carrier
countcarrier1 <- countcarrier
countcarrier1$UniqueCarrier <-factor(countcarrier$UniqueCarrier, 
                                     levels=countcarrier[order(countcarrier$Counts), "UniqueCarrier"])

countcarrier1

# installing and loading the required packages of ggplot
install.packages("ggplot2",dependencies = TRUE)
library(gridExtra)
library(grid)
library(ggplot2)

y <-ggplot(countcarrier1, aes(x=UniqueCarrier, y=Counts)) + 
  geom_bar(stat="identity",colour="white",fill="black" )+ 
  ggtitle("Market Share for 30 Airlines(1987-2012)")+theme_bw()+ theme(panel.border = element_blank(), 
                                                                       panel.grid.major = element_blank(),
                                                                       panel.grid.minor = element_blank())+
  labs(x="Airline Carrier",y="Total Scheduled Flights",colours="red")+
  theme(axis.title.x = element_text(colour = "maroon",size=12,face="bold"),
        axis.title.y = element_text(colour = "maroon",size=15,face="bold"))+
  theme(plot.title=element_text(colour = "maroon",size=12,face="bold"))+
  theme(axis.text.y = element_text(angle = 0, hjust = 0,size=7))

results <-rxCube(~OriginState:DestState,returnDataFrame = TRUE, data=bigAirDS)
dim(results)
head(results)

install.packages("dplyr")
library(dplyr)
library(ggplot2)

result <- results %>%
  filter(OriginState!=DestState)
dim(result)


ggplot(result,aes(OriginState, DestState)) +
  geom_tile(aes(fill=Counts)) +
  theme(axis.text.y = element_text(angle = 0, hjust = 0,size=5,face="bold"))+
  theme(axis.text.x = element_text(angle = 90, hjust = 0,size=5,face="bold")) +
  scale_fill_gradient(low = "white", high = "maroon") +
  coord_fixed(ratio=0.9)


####converts ArrDel15 to a factor and retunrs dataframe of flightdelay>15

bigAirDS <- rxFactors(bigAirDS,factorInfo =ArrDel15 )

arr <- rxCube(~UniqueCarrier:as.factor(ArrDel15),returnDataFrame = TRUE,data=bigAirDS)
head(arr,50)
dim(arr)
rxGetInfo(arr,getVarInfo = TRUE)
class(arr)

arr[,2]


install.packages("dplyr")
library(dplyr)

## removing all false values to get arrdelay15 values for all flights
arr15 <- arr %>%
  filter(arr[,2]=='TRUE')


## to get total number of flights for each carrier by using rxsummary
uniq <- rxSummary(~UniqueCarrier ,data=bigAirDS)

# list objects and the counts for the factor columns are stored in an element called categorical . 
#Here's how we can compare the counts for one factor column but this retunrs a list

uniq$categorical[[1]]

# we convert the list to  dataframe to do cbind and other operations
uniqdf <- as.data.frame(uniq$categorical[[1]])

## we have combined total count of floghts data frame and arrdelay>15 dataframe by cbind

uniqcbind <- cbind(arr15,uniqdf)

# just changed the names for calcualtion purpose

names(uniqcbind) <- c('UniqueCarrier','ArrDel15','Counts','UniqCar','Total')

# calcualting percentage of flightdealyed greater than 15 mins for each carrier

uniqcbind$percentage <- (uniqcbind$Counts/uniqcbind$Total)*100


install.packages("ggplot2")
library(ggplot2)

# ordering the percentage column for plot

attach(uniqcbind)

uniqcbind$UniqueCarrier <- factor(UniqueCarrier,levels=uniqcbind[order(percentage),"UniqueCarrier"])

ggplot(uniqcbind,aes(x=UniqueCarrier,y=percentage))+geom_bar(stat="identity",colour="black",fill="maroon")+
  ggtitle("Percentage of Airline Arrival Delays")+theme_bw()+ theme(panel.border = element_blank(),
                                                                    panel.grid.major = element_blank(),
                                                                    panel.grid.minor = element_blank())+
  labs(x="Airline Carrier",y="Percentage of Flights Delayed",colours="red")+
  theme(axis.title.x = element_text(colour = "Black",size=10,face="bold"),
        axis.title.y = element_text(colour = "Black",size=10,face="bold"))+
  theme(plot.title=element_text(colour = "Black",size=10,face="bold"))+
  theme(axis.text.y = element_text(angle = 0, hjust = 0,size=7))

uniqcbind

### calcuating summary for all the delays against airline carrier
delay_summ <-rxSummary(WeatherDelay~UniqueCarrier+NASDelay~UniqueCarrier+SecurityDelay~UniqueCarrier
                       +LateAircraftDelay~UniqueCarrier+CarrierDelay~UniqueCarrier,data=bigAirDS)
delay_summ


# list objects and the counts for the factor columns are stored in an element called categorical . 
#Here's how we can compare the counts for one factor column but this retunrs a list
delay_summ$categorical
class(delay_summ$categorical)

# we convert the list to  dataframe to do cbind and other operations
delaysummdf1 <- as.data.frame(delay_summ$categorical[[1]])
delaysummdf1

# calculating density plots and box plots for all type of delays
par(mfrow=c(2,3))

plot(density(delaysummdf1$Means,na.rm=TRUE),main="Avg Weather delay",
     xlab="Delay in Minutes",col="blue")
boxplot(delaysummdf1$Means,na.rm=TRUE)

delaysummdf2 <- as.data.frame(delay_summ$categorical[[2]])
delaysummdf2

plot(density(delaysummdf2$Means,na.rm=TRUE),main=" Avg NAS delay",
     xlab="Delay in Minutes",col="darkgreen")

boxplot(delaysummdf2$Means,na.rm=TRUE)

delaysummdf3 <- as.data.frame(delay_summ$categorical[[3]])
delaysummdf3

plot(density(delaysummdf3$Means,na.rm=TRUE),main=" Avg Security delay",
     xlab="Delay in Minutes",col="violet")

boxplot(delaysummdf3$Means,na.rm=TRUE)

delaysummdf4 <- as.data.frame(delay_summ$categorical[[4]])
delaysummdf4

plot(density(delaysummdf4$Means,na.rm=TRUE),main=" Avg LateAircraftDelay",
     xlab="Delay in Minutes",col="red")

boxplot(delaysummdf4$Means,na.rm=TRUE)

delaysummdf5 <- as.data.frame(delay_summ$categorical[[5]])
delaysummdf5

plot(density(delaysummdf5$Means,na.rm=TRUE),main="Avg CarrierDelay",
     xlab="Delay in Minutes",col="brown")

boxplot(delaysummdf5$Means,na.rm=TRUE)
attach(delaysummdf5)

# To find average NAS delay at each airport
delay_nasorg <-rxSummary(NASDelay~OriginAirportID,data=bigAirDS)
delay_nasorg

delay_nasorgdf <- as.data.frame(delay_nasorg$categorical)
dim(delay_nasorgdf)

delay_order <- delay_nasorgdf[order(delay_nasorgdf$Means, decreasing=TRUE) ]
top10 <- head(delay_order,10)

# total no of flights opearting at each airport
delay_airportg <-rxSummary(~OriginAirportID,data=bigAirDS)
delay_airportgdf <- as.data.frame(delay_airportg$categorical)
class(delay_airportgdf)
names(delay_airportgdf)
attach(delay_airportgdf)


### arranging desc by counts
delay_airportgdfar <- arrange(delay_airportgdf, desc(Counts))

h1 <- head(delay_airportgdfar)

# plotting NAS delay means
par(mfrow=c(1,1))

plot(density(delay_nasorgdf$Means,na.rm=TRUE),main=" Avg NAS delay",
     xlab="Delay in Minutes",col="red")

boxplot(delay_nasorgdf$Means,na.rm=TRUE,col="blue",ylab="minutes")

bind <- cbind(delay_nasorgdf,delay_airportgdf)
head(bind)
tail(bind)

# finding coorealtion between total number of flights at airport and avg NAS delay
cor(log(bind$Counts),bind$Means,use="complete.obs")
plot(log(bind$Counts),bind$Means,xlab="log of total flights at the airport",
     ylab="Mean delay in minutes",col="blue")


## checking which airline carrier has more avg carrier delay in minutes
delaysummdf5$UniqueCarrier <- factor(UniqueCarrier,levels=delaysummdf5[order(Means),"UniqueCarrier"])

ggplot(delaysummdf5,aes(x=UniqueCarrier,y=Means))+geom_bar(stat="identity")


delay_summarr_dep <-rxSummary(ArrDelay~UniqueCarrier+DepDelay~UniqueCarrier,data=bigAirDS)
delay_summarr_dep

## calculating logistic regression
logitObj <- rxLogit(ArrDel15~DayOfWeek + UniqueCarrier, data = bigAirDS)
summary(logitObj)

predictDS <- rxPredict(modelObject = logitObj, data = bigAirDS)
rxGetInfo(predictDS, getVarInfo=TRUE, numRows=5)

arrDelayLm2 <- rxLinMod(ArrDelay ~ DayOfWeek + UniqueCarrier + Dest,
                        data = bigAirDS)



mySparkCluster <- RxSpark(consoleOutput=TRUE)
rxSetComputeContext(mySparkCluster)
hdfsFS <- RxHdfsFileSystem()
bigDataDirRoot <- "/AirOnTime"
inputDir <- file.path(bigDataDirRoot,"AirlineDataSubsample.xdf")
bigAirDS1 <- RxXdfData( inputDir,fileSystem =hdfsFS)
rxGetInfo(bigAirDS1,getVarInfo = TRUE)

rxPredict(arrDelayLm2, data = bigAirDS1, outData = bigAirDS1)

