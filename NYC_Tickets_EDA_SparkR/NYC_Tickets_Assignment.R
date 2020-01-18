##############################################################################################
##                                                                                          ##
##   Project     : NYC PARKING TICKETS - AN EXPLORATORY ANALYSIS                            ##
##                                                                                          ##
##   Description : New York City is a thriving metropolis. Just like most other metros that ##
##                 size, one of the biggest problems its citizens face is parking.          ##
##                 We have to analyse the NYC Parking Tickets Data for the year 2017 and    ##
##                 provide meaningful insights on the data.                                 ##
##                                                                                          ##
##   Date        : 10 March 2019                                                            ##
##                                                                                          ##
##   Author      : Deepankar Kotnala                                                        ##
##                                                                                          ##
##############################################################################################

# Spark - NYC dataset

# Remove all R objects - Clean the workspace
rm(list = ls(all = TRUE))

# ------------------------------------------------------------------------------------------------------------#
# Load the required libraries:
library(dplyr)
library(stringr)
library(ggplot2)
library(magrittr)
# ------------------------------------------------------------------------------------------------------------#

# 1. Load SparkR
spark_path <- '/usr/local/spark'
if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = spark_path)
}
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

# Initialise the sparkR session
sparkR.session(master = "yarn-client", sparkConfig = list(spark.driver.memory = "1g"))

# Before executing any hive-sql query from RStudio, you need to add a jar file in RStudio 
sql("ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-hcatalog-core-1.1.0-cdh5.11.2.jar")

# ------------------------------------------------------------------------------------------------------------#
# Create a Spark DataFrame
# ------------------------------------------------------------------------------------------------------------#
nyc_data <- SparkR::read.df("/common_folder/nyc_parking/Parking_Violations_Issued_-_Fiscal_Year_2017.csv", "CSV", 
                            header="true", inferSchema = "true")

# ------------------------------------------------------------------------------------------------------------#
# Understanding the dimensions and structure of the Imported Dataset.
# ------------------------------------------------------------------------------------------------------------#

# Check the Spark Dataframe
head(nyc_data)
#   Summons Number Plate ID Registration State Issue Date Violation Code Vehicle Body Type Vehicle Make Violation Precinct Issuer Precinct Violation Time
# 1     5092469481  GZH7067                 NY 2016-07-10              7              SUBN        TOYOT                  0               0          0143A
# 2     5092451658  GZH7067                 NY 2016-07-08              7              SUBN        TOYOT                  0               0          0400P
# 3     4006265037  FZX9232                 NY 2016-08-23              5              SUBN         FORD                  0               0          0233P
# 4     8478629828  66623ME                 NY 2017-06-14             47              REFG        MITSU                 14              14          1120A
# 5     7868300310  37033JV                 NY 2016-11-21             69              DELV        INTER                 13              13          0555P
# 6     5096917368  FZD8593                 NY 2017-06-13              7              SUBN        ME/BE                  0               0          0852P

# Looking at the dimensions of the dataset.
dim(nyc_data)
# 10803028 rows   
# 10 columns
# ------------------------------------------------------------------------------------------------------------#

# Replacing the white spaces in between consecutive words in column names with "_" 
# so as to prevent any error occurring because of the improper name (white spaces might cause errors).

# Having a look at the column names
colnames(nyc_data)
# [1] "Summons Number"     "Plate ID"        "Registration State"   "Issue Date"   "Violation Code"   "Vehicle Body Type"  "Vehicle Make"  "Violation Precinct"
# [9] "Issuer Precinct"    "Violation Time" 

# Removing white spaces from both the sides, if any.
colnames(nyc_data) <- str_trim(colnames(nyc_data), side= "both")
# Replacing the spaces between the words of column names with "_" (an underscore).
colnames(nyc_data) <- str_replace_all(colnames(nyc_data), pattern=" ", replacement = "_")
# Having a look at the new column names
colnames(nyc_data)
# [1] "Summons_Number"     "Plate_ID"         "Registration_State"  "Issue_Date"   "Violation_Code"   "Vehicle_Body_Type"  "Vehicle_Make"  "Violation_Precinct"
# [9] "Issuer_Precinct"    "Violation_Time"

# ------------------------------------------------------------------------------------------------------------#
printSchema(nyc_data)

# ------------------------------------------------------------------------------------------------------------#
# Exploratory Data Analysis and Data Cleaning
# ------------------------------------------------------------------------------------------------------------#

# Checking for duplicates and removing them, if any.
nrow(nyc_data)
# 10803028  
nyc_data<- dropDuplicates(nyc_data, "Summons_Number")
nrow(nyc_data)
# 10803028  
# There were no duplicate records present in the dataset. We have checked for duplicates using the dropDuplicates functionality 
# on "Summons_Number" column which acts as a primary key here.

# ------------------------------------------------------------------------------------------------------------#

# Filtering out the data only for year 2017
# nyc_data <- nyc_data[nyc_data$Issue_Date >= "2017-01-01" & nyc_data$Issue_Date <= "2017-12-31"]

createOrReplaceTempView(nyc_data, "NYCParking_data_tbl")
nyc_data <- SparkR::sql("SELECT * FROM NYCParking_data_tbl WHERE Year(issue_date) = '2017' ")

nrow(nyc_data)
# 5431918 records are present now which belong to the year 2017.


# Setting the theme for plots

plot_theme <- theme_bw() + 
                 theme(axis.text.y=element_text(size=10)) +
                 theme(legend.text=element_text(size=10)) +
                 theme(legend.title=element_text(size=10)) 

# ------------------------------------------------------------------------------------------------------------#
#                                             Answers to the questions                                        #
# ------------------------------------------------------------------------------------------------------------#

# Examine the Data


# ------------------------------------------------------------------------------------------------------------#
# Solution 1. 
# ------------------------------------------------------------------------------------------------------------#
n <- nrow(nyc_data)
n
# Total number of tickets are: 5431918
# ------------------------------------------------------------------------------------------------------------#


# ------------------------------------------------------------------------------------------------------------#
# Solution 2. 
# ------------------------------------------------------------------------------------------------------------#
Registration_State_Group <- SparkR::sql("SELECT Registration_State, count(*) as Frequency_of_Tickets
                                        from nyc_data_view 
                                        group by Registration_State
                                        order by Frequency_of_Tickets desc")
head(Registration_State_Group,15)

# Only the top 15 output rows are shown in below comments for code brievity.

#    Registration_State count(Registration_State)
# 1                  NY                   4273951
# 2                  NJ                    475825
# 3                  PA                    140286
# 4                  CT                     70403
# 5                  FL                     69468
# 6                  IN                     45525
# 7                  MA                     38941
# 8                  VA                     34367
# 9                  MD                     30213
# 10                 NC                     27152
# 11                 TX                     18827
# 12                 IL                     18666
# 13                 GA                     17537
# 14                 99                     16055
# 15                 AZ                     12379

# We can see that Registration_State column contains "99" category as well, which needs to be replaced with the state having maximum entries.
# So we will replace the records having "99" as state code with "NY" as the state code (NY is the state having maximum entries - 4273951 entries).

nyc_data$Registration_State <- ifelse(nyc_data$Registration_State == "99", "NY", nyc_data$Registration_State)

# Replacing the view after changing the state code from "99" to "NY".
createOrReplaceTempView(nyc_data, "nyc_data_view")

# Checking whether the values got replaced or not.
Registration_State_Group <- SparkR::sql("SELECT Registration_State, count(*) as Frequency_of_Tickets
                                        from nyc_data_view 
                                        group by Registration_State
                                        order by Frequency_of_Tickets desc")
head(Registration_State_Group,15)

#    Registration_State Frequency_of_Tickets                                      
# 1                  NY              4290006
# 2                  NJ               475825
# 3                  PA               140286
# 4                  CT                70403
# 5                  FL                69468
# 6                  IN                45525
# 7                  MA                38941
# 8                  VA                34367
# 9                  MD                30213
# 10                 NC                27152
# 11                 TX                18827
# 12                 IL                18666
# 13                 GA                17537
# 14                 AZ                12379
# 15                 OH                12281

# Registration_State NY count + 99 code Count = 4273951 + 16055 = 4290006.



# ------------------------------------------------------------------------------------------------------------#
#                                                                                                             #
#                                                Aggregation tasks                                            #
#                                                                                                             #
# ------------------------------------------------------------------------------------------------------------#

# ------------------------------------------------------------------------------------------------------------#
# Solution 1.
# ------------------------------------------------------------------------------------------------------------#
Violation_Code_Group <- SparkR::sql("SELECT Violation_Code, count(*) as Frequency_of_Tickets
                                        from nyc_data_view 
                                       group by Violation_Code
                                       order by Frequency_of_Tickets desc")
head(Violation_Code_Group, 5)


# The top five Violation Codes are given below : 

#     Violation_Code count(Violation_Code)                                         
# 1               21                768087
# 2               36                662765
# 3               38                542079
# 4               14                476664
# 5               20                319646
# ------------------------------------------------------------------------------------------------------------#



# ------------------------------------------------------------------------------------------------------------#
# Solution 2. 
# ------------------------------------------------------------------------------------------------------------#
Vehicle_Body_Type_Group <- SparkR::sql("SELECT Vehicle_Body_Type, count(*) as Frequency_of_Tickets
                                        from nyc_data_view 
                                        group by Vehicle_Body_Type
                                        order by Frequency_of_Tickets desc")
head(Vehicle_Body_Type_Group, 5)

#   Below is the frequency of the top five "Vehicle Body Type" getting a parking ticket.
#   Vehicle_Body_Type count(Vehicle_Body_Type)                                    
# 1              SUBN                  1883954
# 2              4DSD                  1547312
# 3               VAN                   724029
# 4              DELV                   358984
# 5               SDN                   194197
# ------------------------------------------------------------------------------------------------------------#

Vehicle_Make_Group <- SparkR::sql("SELECT Vehicle_Make, count(*) as Frequency_of_Tickets
                                   from nyc_data_view 
                                   group by Vehicle_Make
                                   order by Frequency_of_Tickets desc")
head(Vehicle_Make_Group, 5)

#  Below is the frequency of the top five "Vehicle Make" getting a parking ticket.
#  Vehicle_Make count(Vehicle_Make)                                              
#1         FORD              636844
#2        TOYOT              605291
#3        HONDA              538884
#4        NISSA              462017
#5        CHEVR              356032

# ------------------------------------------------------------------------------------------------------------#



# Solution 3. ------------------------------------------------------------------------------------------------#

# ------------------------------------------------------------------------------------------------------------#
# 3.(a)
# ------------------------------------------------------------------------------------------------------------#
Violation_Precinct_Group <- SparkR::sql("SELECT Violation_Precinct, count(*) as Frequency
                                      from nyc_data_view 
                                      group by Violation_Precinct
                                      order by Frequency desc")
head(Violation_Precinct_Group,6)
#   Violation_Precinct Frequency_of_Tickets                                       
# 1                  0               925596
# 2                 19               274445
# 3                 14               203553
# 4                  1               174702
# 5                 18               169131
# 6                114               147444

# Precinct 0 is the erroneous entry. The top five Precincts for Violations are given below.

Violation_Precinct_Group_top5 <- data.frame(head(Violation_Precinct_Group,6))

Violation_Precinct_Group_top5 <- Violation_Precinct_Group_top5[c(2:6),]

Violation_Precinct_Group_top5
#   Violation_Precinct Frequency
# 2                 19    274445
# 3                 14    203553
# 4                  1    174702
# 5                 18    169131
# 6                114    147444

ggplot(Violation_Precinct_Group_top5, aes(x=as.factor(Violation_Precinct), y=Frequency))+ 
                            geom_col() + xlab("Violation Precinct") + ylab("Frequency") + 
                            ggtitle("Plot of Top Five Violation Precincts vs the Frequency of Tickets") + 
                            geom_text(aes(label=Frequency), vjust=-0.3) +
                            plot_theme 

# Precinct 19 (Region 19) is having the highest frequency of Violations with 274445 tickets.


# ------------------------------------------------------------------------------------------------------------#
# 3.(b)
# ------------------------------------------------------------------------------------------------------------#
Issuer_Precinct_Group <- SparkR::sql("SELECT Issuer_Precinct, count(*) as Frequency
                                      from nyc_data_view 
                                      group by Issuer_Precinct
                                      order by Frequency desc")
head(Issuer_Precinct_Group,6)
#   Issuer_Precinct count(Issuer_Precinct)                                        
# 1               0                1078406
# 2              19                 266961
# 3              14                 200495
# 4               1                 168740
# 5              18                 162994
# 6             114                 144054

Issuer_Precinct_Group_top5 <- data.frame(head(Issuer_Precinct_Group,6))

Issuer_Precinct_Group_top5 <- Issuer_Precinct_Group_top5[c(2:6),]

ggplot(Issuer_Precinct_Group_top5, aes(x=as.factor(Issuer_Precinct), y=Frequency)) +
                         geom_col() + xlab("Issuer Precinct") + ylab("Frequency")  + 
                         ggtitle("Plot of Top Five Issuer Precincts vs the Frequency of Tickets") + 
                         geom_text(aes(label=Frequency), vjust=-0.3) + 
                         plot_theme
                         

# Observations:
# Precinct 19 (Region 19) is having the highest frequency of Violations with 274445 tickets.
# Highest number of tickets are issued in Precinct 19 (Region 19)

# ------------------------------------------------------------------------------------------------------------#


# ------------------------------------------------------------------------------------------------------------#
# Solution 4.
# ------------------------------------------------------------------------------------------------------------#

# Creating the SQL view:
createOrReplaceTempView(nyc_data, "nyc_data_view")

# Finding the Violation_Code frequency for Precinct 19
Issuer_Precinct_19_VC_Freq <- SparkR::sql("SELECT Violation_Code, count(*) as Frequency
                                           FROM nyc_data_view
                                           WHERE Issuer_Precinct = 19
                                           GROUP BY Violation_Code
                                           ORDER BY count(*) DESC")
head(Issuer_Precinct_19_VC_Freq, 10)
#    Violation_Code Frequency                                                     
# 1              46     48445
# 2              38     36386
# 3              37     36056
# 4              14     29797
# 5              21     28415
# 6              20     14629
# 7              40     11416
# 8              16      9926
# 9              71      7493
# 10             19      6856

# Finding the Violation_Code frequency for Precinct 14
Issuer_Precinct_14_VC_Freq <- SparkR::sql("SELECT Violation_Code, count(*) as Frequency
                                           FROM nyc_data_view
                                           WHERE Issuer_Precinct = 14
                                           GROUP BY Violation_Code
                                           ORDER BY count(*) DESC")
head(Issuer_Precinct_14_VC_Freq, 10)
#    Violation_Code Frequency                                                     
# 1              14     45036
# 2              69     30464
# 3              31     22555
# 4              47     18364
# 5              42     10027
# 6              46      7679
# 7              19      7031
# 8              84      6743
# 9              82      5052
# 10             40      3582

# Finding the Violation_Code frequency for Precinct 1
Issuer_Precinct_1_VC_Freq <-SparkR::sql("SELECT Violation_Code, count(*) as Frequency
                                         FROM nyc_data_view
                                         WHERE Issuer_Precinct = 1
                                         GROUP BY Violation_Code
                                         ORDER BY count(*) DESC")
head(Issuer_Precinct_1_VC_Freq, 10)
#    Violation_Code Frequency                                                     
# 1              14     38354
# 2              16     19081
# 3              20     15408
# 4              46     12745
# 5              38      8535
# 6              17      7526
# 7              37      6470
# 8              31      5853
# 9              69      5672
# 10             19      5375

# Plots for the above observations:

Issuer_Precinct_19_VC_Freq <- data.frame(head(Issuer_Precinct_19_VC_Freq,10))
ggplot(Issuer_Precinct_19_VC_Freq, aes(x=as.factor(Violation_Code), y=Frequency)) +
                                   geom_col() + xlab("Violation_Code") + ylab("Frequency")  + 
                                   ggtitle("Plot of Frequency for top 10 Violation Code of Precinct 19") + 
                                   geom_text(aes(label=Frequency), vjust=-0.3) + 
                                   plot_theme

Issuer_Precinct_14_VC_Freq <- data.frame(head(Issuer_Precinct_14_VC_Freq,10))
ggplot(Issuer_Precinct_14_VC_Freq, aes(x=as.factor(Violation_Code), y=Frequency)) +
                                   geom_col() + xlab("Violation_Code") + ylab("Frequency")  + 
                                   ggtitle("Plot of Frequency for top 10 Violation Code of Precinct 14") + 
                                   geom_text(aes(label=Frequency), vjust=-0.3) + 
                                   plot_theme

Issuer_Precinct_1_VC_Freq <- data.frame(head(Issuer_Precinct_1_VC_Freq,10))

ggplot(Issuer_Precinct_1_VC_Freq, aes(x=as.factor(Violation_Code), y=Frequency)) +
                                  geom_col() + xlab("Violation_Code") + ylab("Frequency")  + 
                                  ggtitle("Plot of Frequency for top 10 Violation Code of Precinct 1") + 
                                  geom_text(aes(label=Frequency), vjust=-0.3) + 
                                  plot_theme

# ------------------------------------------------------------------------------------------------------------#
# Observations:
# ------------------------------------------------------------------------------------------------------------#
# (1) Yes, these precinct zones have an exceptionally high frequency of certain violation codes.

# (2) Violation Code 14 is having the very high frequency across all the three Precincts (i.e, Precinct 1, Precinct 14 and Precinct 19)
#     The frequency of Violation Code 14 is exceptionally high in Precinct 1 and Precinct 14. It is very high in Precinct 19 (frequency = 29797).

# (3) Violation Code 46 is having an exceptionally high frequenct in Precinct 19 (Frequency = 48445)
#     Also, Violation Code 37 and 38 are having high frequency in Precinct 19. (VC* 37 - Frequency 36056, and VC* 38 - frequency 36386)
#     *[VC stands for Violation Code]
# ------------------------------------------------------------------------------------------------------------#
# ------------------------------------------------------------------------------------------------------------#


# ------------------------------------------------------------------------------------------------------------#
# Solution 5. 

# ------------------------------------------------------------------------------------------------------------#
# Solution 5 (a)
# ------------------------------------------------------------------------------------------------------------#

# Check for missing values (NULL values) in the dataset.

createOrReplaceTempView(nyc_data, "nyc_data_view")
count_of_null_values <- SparkR::sql("select count(*) row_cnt,
                                    sum(case when Summons_Number is null
                                    then 1 
                                    else 0 
                                    end) Summons_Number_Null_Count, 
                                    sum(case when Plate_ID is null 
                                    then 1 
                                    else 0 
                                    end) Plate_ID_Null_Count, 
                                    sum(case when Registration_State is null 
                                    then 1 
                                    else 0 
                                    end) Registration_State_Null_Count, 
                                    sum(case when Issue_Date is null 
                                    then 1 
                                    else 0 
                                    end) Issue_Date_Null_Count,
                                    sum(case when Violation_Code is null 
                                    then 1 
                                    else 0 
                                    end) Violation_Code_Null_Count, 
                                    sum(case when Vehicle_Body_Type is null 
                                    then 1 
                                    else 0 
                                    end) Vehicle_Body_Type_Null_Count, 
                                    sum(case when Registration_State is null 
                                    then 1 
                                    else 0 
                                    end) Registration_State_Null_Count, 
                                    sum(case when Vehicle_Make is null 
                                    then 1 
                                    else 0 
                                    end) Vehicle_Make_Null_Count,
                                    sum(case when Violation_Precinct is null 
                                    then 1 
                                    else 0 
                                    end) Violation_Precinct_Null_Count, 
                                    sum(case when Issuer_Precinct is null 
                                    then 1 
                                    else 0 
                                    end) Issuer_Precinct_Null_Count, 
                                    sum(case when Violation_Time is null 
                                    then 1 
                                    else 0 
                                    end) Violation_Time_Null_Count
                                    from nyc_data_view")

head(count_of_null_values)
# There are no NULL values in the dataset.



# ------------------------------------------------------------------------------------------------------------#
# Solution 5(b)
# ------------------------------------------------------------------------------------------------------------#

# Converting the Violation_Time column to a proper format.
nyc_data$temp <- "M"
nyc_data$Violation_Time<-concat(nyc_data$Violation_Time, nyc_data$temp)
nyc_data <- drop(nyc_data, c("temp"))

head(nyc_data,20)

# printSchema(nyc_data)

#Extracting Violation Hour, Violation Minute and Part of Day.
nyc_data$Violation_Hour   <- substr(nyc_data$Violation_Time, 1, 2)
nyc_data$Violation_Minute <- substr(nyc_data$Violation_Time, 3, 4)
nyc_data$Violation_AMPM   <- substr(nyc_data$Violation_Time, 5, 6)

nyc_data$Violation_Hour <- cast(nyc_data$Violation_Hour,dataType = "int")
nyc_data$Violation_Minute <- cast(nyc_data$Violation_Minute,dataType = "int")

printSchema(nyc_data)
head(nyc_data,20)

# nyc_data$Violation_Time <- to_timestamp(x = nyc_data$Violation_Time, format = "hhmma")

# Checking the count of records where Violation_Time column is having inconsistent values.
createOrReplaceTempView(nyc_data, "nyc_data_view")
Inconsistencies_In_Hour <- SparkR::sql("SELECT Violation_Hour, Violation_AMPM, count(*) as Frequency
                                         FROM nyc_data_view
                                         WHERE Violation_Hour = 00 AND Violation_AMPM = 'PM' 
                                         OR  Violation_Hour = 12 AND Violation_AMPM = 'AM'
                                         OR Violation_Hour < 0
                                         GROUP BY Violation_Hour, Violation_AMPM
                                         ORDER By Violation_Hour")
head(Inconsistencies_In_Hour, 5)
#   Violation_Hour Violation_AMPM Frequency                                       
# 1             00             PM       119
# 2             12             AM     17236

# Converting the Violation_Hour column to 24HR format.
nyc_data$Violation_Hour <- ifelse(nyc_data$Violation_AMPM =="AM" & nyc_data$Violation_Hour ==12, 00, nyc_data$Violation_Hour)
nyc_data$Violation_AMPM <- ifelse(nyc_data$Violation_AMPM =="PM" & nyc_data$Violation_Hour ==00, "AM", nyc_data$Violation_AMPM)

createOrReplaceTempView(nyc_data, "nyc_data_view")
Convert_to_24hr_time <- SparkR::sql("SELECT Violation_Hour, Violation_AMPM, count(*) as Frequency
                                            FROM nyc_data_view
                                            WHERE Violation_Hour < 12 AND  Violation_AMPM = 'PM' 
                                            GROUP BY Violation_Hour, Violation_AMPM")
head(Convert_to_24hr_time, 20)

#    Violation_Hour Violation_AMPM Frequency                                      
# 1              02             PM    466068
# 2              10             PM     42540
# 3              12             PM    510135
# 4              06             PM    104284
# 5              07             PM     26100
# 6              08             PM     49221
# 7              03             PM    314468
# 8              05             PM    211173
# 9              01             PM    549287
# 10             09             PM     55322
# 11             04             PM    295983
# 12             11             PM     29277

# Converting Violation_Hour to 24HR format
nyc_data$Violation_Hour <- ifelse(nyc_data$Violation_AMPM =="PM" & nyc_data$Violation_Hour < 12, 
                                  nyc_data$Violation_Hour+12, nyc_data$Violation_Hour)

createOrReplaceTempView(nyc_data, "nyc_data_view")
Inconsistencies_In_24hr <- SparkR::sql("SELECT count(*) as Count_of_Records
                                         FROM nyc_data_view
                                         WHERE Violation_Hour > 23 
                                         AND Violation_AMPM = 'PM' ")
head(Inconsistencies_In_24hr)
#  Count_of_Records                                                              
#                42

# There are 42 records having hour value more than 23. We will replace these values with "23".

nyc_data$Violation_Hour <- ifelse(nyc_data$Violation_Hour > 23, 23, nyc_data$Violation_Hour)


#************************************************************************************************************#
createOrReplaceTempView(nyc_data, "nyc_data_view")
nyc_data <- SparkR::sql("SELECT * FROM nyc_data_view
                                  WHERE Violation_Time not like 'na%' ")
nrow(nyc_data)
# 5431902
# We have removed those rows where Violation_Time column was having "nanM" value.
#************************************************************************************************************#

# Dropping the column Violation_AMPM since it is no longer required.
nyc_data <- drop(nyc_data, c("Violation_AMPM"))

# Check for inconsistencies
createOrReplaceTempView(nyc_data, "nyc_data_view")
Inconsistency_check_in_time <- SparkR::sql("SELECT distinct(Violation_Hour)
                                            FROM nyc_data_view")
head(Inconsistency_check_in_time,100)

# All data inconsistencies in Violation_Hour have been taken care of.

head(nyc_data)
printSchema(nyc_data)
# ------------------------------------------------------------------------------------------------------------#


# ------------------------------------------------------------------------------------------------------------#
#Solution 5(c)
# ------------------------------------------------------------------------------------------------------------#
#Divide 24 hours into 6 equal discrete bins of time. The intervals you choose are at your discretion. 

createOrReplaceTempView(nyc_data, "nyc_data_view")
Violation_Hour_Bin <- SparkR::sql("SELECT Violation_Hour,
                                  Violation_Code,
                                  CASE WHEN Violation_Hour BETWEEN 0 AND 3
                                  THEN '0_3'
                                  WHEN Violation_Hour BETWEEN 4 AND 7
                                  THEN '4_7'
                                  WHEN Violation_Hour BETWEEN 8 AND 11
                                  THEN '8_11'
                                  WHEN Violation_Hour BETWEEN 12 AND 15
                                  THEN '12_15' 
                                  WHEN Violation_Hour BETWEEN 16 AND 19
                                  THEN '16_19' 
                                  WHEN Violation_Hour BETWEEN 20 AND 23
                                  THEN '20_23' 
                                  ELSE '24_29'
                                  END AS Violation_Hour_Bin
                                  FROM nyc_data_view")

head(Violation_Hour_Bin)


Violation_Hour_Bin <- SparkR::sql("SELECT Violation_Hour,
                                          Violation_Code,
                                          CASE WHEN Violation_Hour BETWEEN 0 AND 3 THEN '0_3'
                                          WHEN Violation_Hour BETWEEN 4 AND 7 THEN '4_7'
                                          WHEN Violation_Hour BETWEEN 8 AND 11 THEN '8_11'
                                          WHEN Violation_Hour BETWEEN 12 AND 15 THEN '12_15' 
                                          WHEN Violation_Hour BETWEEN 16 AND 19 THEN '16_19' 
                                          WHEN Violation_Hour BETWEEN 20 AND 23 THEN '20_23' 
                                          END AS Violation_Hour_Bin
                                          FROM nyc_data_view")

createOrReplaceTempView(Violation_Hour_Bin, "Violation_Hour_BinView")
Violation_Hour_Bin_Group <- SparkR::sql("SELECT Violation_Hour_Bin,
                                        Violation_Code,
                                        Frequency_of_Tickets
                                        FROM (SELECT Violation_Hour_Bin,
                                        Violation_Code,
                                        Frequency_of_Tickets,
                                        dense_rank() over (partition by Violation_Hour_Bin order by Frequency_of_Tickets desc) dense_rank_value
                                        FROM (SELECT Violation_Hour_Bin,
                                        Violation_Code,
                                        count(*)as Frequency_of_Tickets
                                        FROM Violation_Hour_BinView
                                        GROUP BY Violation_Hour_Bin,
                                        Violation_Code))
                                        WHERE dense_rank_value <= 3")
head(Violation_Hour_Bin_Group,20)

#    Violation_Hour_Bin Violation_Code Frequency_of_Tickets                       
# 1               16_19             38               102855
# 2               16_19             14                75902
# 3               16_19             37                70345
# 4                8_11             21               598070
# 5                8_11             36               348165
# 6                8_11             38               176570
# 7                 4_7             14                74114
# 8                 4_7             40                60652
# 9                 4_7             21                57897
# 10              12_15             36               286284
# 11              12_15             38               240721
# 12              12_15             37               167025
# 13                0_3             21                36960
# 14                0_3             40                25881
# 15                0_3             78                15534
# 16              20_23              7                26293
# 17              20_23             40                22341
# 18              20_23             14                21046

# Above are the most commonly occurring violations for different bins of time throughout the 24 hours of the day.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------


# ------------------------------------------------------------------------------------------------------------#
# Solution 5(d) . 
# ------------------------------------------------------------------------------------------------------------#
Three_Most_Common_Violations <- SparkR::sql("SELECT Violation_Code,
                                     count(*) no_of_tickets
                                     FROM Violation_Hour_BinView
                                     GROUP BY Violation_Code
                                     ORDER BY no_of_tickets desc")

head(Three_Most_Common_Violations,3)
#   Violation_Code no_of_tickets                                                  
# 1             21        768087
# 2             36        662765
# 3             38        542079

# Top three Violation Codes are 21, 36 and 38

# ------------------------------------------------------------------------------------------------------------#



# ------------------------------------------------------------------------------------------------------------#
# Solution 6.
# ------------------------------------------------------------------------------------------------------------#

# ------------------------------------------------------------------------------------------------------------#
# Solution 6(a).
# ------------------------------------------------------------------------------------------------------------#
nyc_data$Issue_Year <- year(nyc_data$Issue_Date)
nyc_data$Issue_Month <- month(nyc_data$Issue_Date)

createOrReplaceTempView(nyc_data, "nyc_data_view")
Season_Bins    <- SparkR::sql("SELECT Summons_Number,
                               Violation_Code,(CASE WHEN Issue_Month IN (1,2,12) THEN 'Winter' 
                               WHEN Issue_Month BETWEEN 3 AND 5 THEN 'Spring' 
                               WHEN Issue_Month BETWEEN 6 AND 8 THEN 'Summer' 
                               WHEN Issue_Month BETWEEN 9 AND 12 THEN 'Fall' 
                               END) AS Season FROM nyc_data_view")

createOrReplaceTempView(Season_Bins, "Season_Violations_Ticket")

Season_Violations_Ticket_Group <- SparkR::sql("SELECT Season,
                                               COUNT(*) as Frequency
                                               FROM Season_Violations_Ticket
                                               GROUP BY Season
                                               ORDER BY Frequency desc")
head(Season_Violations_Ticket_Group, 4)

#   Season Frequency                                                              
# 1 Spring   2873371
# 2 Winter   1704686
# 3 Summer    852866
# 4   Fall       979

# Above is a list of Seasons throughout the year along with the frequency of tickets in each season.
# Highest number of tickets are issued in Spring, followed by Winter, Summer and Fall.

# ------------------------------------------------------------------------------------------------------------#


# ------------------------------------------------------------------------------------------------------------#
# Solution 6(b).
# ------------------------------------------------------------------------------------------------------------#

# Finding the three most common violations for each of these four seasons.


# Finding the Violation_Code frequency for Spring Season
Spring_VC_Freq <- SparkR::sql("SELECT Violation_Code, count(*) as Frequency
                               FROM Season_Violations_Ticket
                               WHERE Season = 'Spring'
                               GROUP BY Violation_Code
                               ORDER BY count(*) DESC")
head(Spring_VC_Freq, 3)
#    Violation_Code Frequency                                                     
# 1              21    402424
# 2              36    344834
# 3              38    271167

# Three most common violations for Spring Season are: 21, 36 and 38.
# ------------------------------------------------------------------------------------------------------------#

# Finding the Violation_Code frequency for Winter Season
Winter_VC_Freq <- SparkR::sql("SELECT Violation_Code, count(*) as Frequency
                               FROM Season_Violations_Ticket
                               WHERE Season = 'Winter'
                               GROUP BY Violation_Code
                               ORDER BY count(*) DESC")
head(Winter_VC_Freq, 3)
#   Violation_Code Frequency                                                      
# 1             21    238183
# 2             36    221268
# 3             38    187386

# Three most common violations for Spring Season are: 21, 36 and 38.
# ------------------------------------------------------------------------------------------------------------#

# Finding the Violation_Code frequency for Summer Season
Summer_VC_Freq <- SparkR::sql("SELECT Violation_Code, count(*) as Frequency
                               FROM Season_Violations_Ticket
                               WHERE Season = 'Summer'
                               GROUP BY Violation_Code
                               ORDER BY count(*) DESC")
head(Summer_VC_Freq, 3)
#   Violation_Code Frequency                                                      
# 1             21    127352
# 2             36     96663
# 3             38     83518
# Three most common violations for Spring Season are: 21, 36 and 38.
# ------------------------------------------------------------------------------------------------------------#

# Finding the Violation_Code frequency for Spring Season
Fall_VC_Freq <- SparkR::sql("SELECT Violation_Code, count(*) as Frequency
                               FROM Season_Violations_Ticket
                               WHERE Season = 'Fall'
                               GROUP BY Violation_Code
                               ORDER BY count(*) DESC")
head(Fall_VC_Freq, 3)
#   Violation_Code Frequency                                                      
# 1             46       231
# 2             21       128
# 3             40       116

# Three most common violations for Spring Season are: 46, 21 and 40.
# ------------------------------------------------------------------------------------------------------------#



# ------------------------------------------------------------------------------------------------------------#
# Solution 7.
# ------------------------------------------------------------------------------------------------------------#

# ------------------------------------------------------------------------------------------------------------#
# Solution 7(a)
# ------------------------------------------------------------------------------------------------------------#
# Total occurrences of the three most common Violation Codes
createOrReplaceTempView(nyc_data, "nyc_data_view")
Top_Three_Violation_Codes_Freq <- SparkR::sql("SELECT Violation_Code, count(*) AS Frequency
                                              FROM nyc_data_view 
                                              GROUP BY Violation_Code
                                              ORDER BY Frequency DESC")
head(Top_Three_Violation_Codes_Freq, 3)

#   Violation_Code Frequency                                                      
# 1             21    768087
# 2             36    662765
# 3             38    542079

# Above are the frequencies of the top three most commonly occurring Violation Codes.

# ------------------------------------------------------------------------------------------------------------#
# Solution 7(b)
# ------------------------------------------------------------------------------------------------------------#
# We will take the average of the two fine values which is provided in the below link for the respective Violation Codes.
# Link : https://www1.nyc.gov/site/finance/vehicles/services-violation-codes.page

Fines <- SparkR::sql("SELECT Violation_Code,
                      (CASE WHEN Violation_Code=21 THEN 55 
                      WHEN Violation_Code=36 THEN 50 
                      WHEN Violation_Code=38 THEN 50 
                      END) AS Fine FROM nyc_data_view
                      WHERE Violation_Code=21 
                      OR Violation_Code=36 
                      OR Violation_Code=38")

head(Fines,50)
# ------------------------------------------------------------------------------------------------------------#


# ------------------------------------------------------------------------------------------------------------#
# Solution 7(c)
# ------------------------------------------------------------------------------------------------------------#
createOrReplaceTempView(Fines, "Fines_tbl")
Total_Fine_Per_Code <- SparkR::sql("SELECT Violation_Code,
                                    SUM(Fine) FROM Fines_tbl
                                    WHERE Violation_Code=21 
                                    OR Violation_Code=36 
                                    OR Violation_Code=38
                                    GROUP BY Violation_Code
                                    ORDER BY SUM(Fine) DESC")
head(Total_Fine_Per_Code)

#   Violation_Code sum(Fine)                                                      
# 1             21  42244785
# 2             36  33138250
# 3             38  27103950

# The total collection of fine for the top three Violation Codes are:
# Violation_Code 21 : $42244785
# Violation_Code 36 : $33138250
# Violation_Code 38 : $27103950

# Violation Code 21 is having the highest sum of fine collected (Fine = 42244785)


# ------------------------------------------------------------------------------------------------------------#
# Solution 7(d)
# ------------------------------------------------------------------------------------------------------------#

# Violation Code 21 is the most commonly occurring Violation and it's total collection of Fine is the highest at $42244785 for the year 2017.

# ------------------------------------------------------------------------------------------------------------#


# sparkR.stop
