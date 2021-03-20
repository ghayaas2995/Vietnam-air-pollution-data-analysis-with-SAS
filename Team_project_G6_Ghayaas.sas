*Importing the HCMC datasets into sas*;

/*1st file:*/
PROC IMPORT OUT=HCMC_2016_12
		DATAFILE='/folders/myfolders/DANA-Group-6-team-project/HoChiMinhCity_PM2.5_2016_12_MTD.csv' 
		DBMS=CSV REPLACE; GETNAMES=YES;	DATAROW=2;
RUN;

/*2nd file:*/
PROC IMPORT OUT=HCMC_2017_12
		DATAFILE='/folders/myfolders/DANA-Group-6-team-project/HoChiMinhCity_PM2.5_2017_12_MTD.csv' 
		DBMS=CSV REPLACE; GETNAMES=YES;	DATAROW=2;
RUN;

/*3rd file:*/
PROC IMPORT OUT=HCMC_2018_12
		DATAFILE='/folders/myfolders/DANA-Group-6-team-project/HoChiMinhCity_PM2.5_2018_12_MTD.csv' 
		DBMS=CSV REPLACE; GETNAMES=YES;	DATAROW=2;
RUN;

/*4th file:*/
PROC IMPORT OUT=HCMC_2019_12
		DATAFILE='/folders/myfolders/DANA-Group-6-team-project/HoChiMinhCity_PM2.5_2019_12_MTD.csv' 
		DBMS=CSV REPLACE; GETNAMES=YES;	DATAROW=2;
RUN;

/*5th file:*/
PROC IMPORT OUT=HCMC_2020_12
		DATAFILE='/folders/myfolders/DANA-Group-6-team-project/HoChiMinhCity_PM2.5_2020_12_MTD.csv' 
		DBMS=CSV REPLACE; GETNAMES=YES;	DATAROW=2;
RUN;

/*6th file:*/
PROC IMPORT OUT=HCMC_2021_02
		DATAFILE='/folders/myfolders/DANA-Group-6-team-project/HoChiMinhCity_PM2.5_2021_02_MTD.csv' 
		DBMS=CSV REPLACE; GETNAMES=YES;	DATAROW=2;
RUN;

********************************************************************************************************************************;
* CLEANING 2016 MONTH DATASET*;
********************************************************************************************************************************;

* lets first explore contents and means of this dataset*;

proc contents data=HCMC_2016_12;
run;

* Exploring all the numeric variables with proc means procedure*;

PROC MEANS DATA=HCMC_2016_12 n nmiss mean median min max skewness kurtosis std mode range;
VAR _numeric_;
RUN;

* Exploring Character variables*;

proc freq data=HCMC_2016_12 order=freq ;
table _char_ ;
run;

* Issues:-
2 values in raw conc = 985 and those 2 values 'Inval' in QC_name variable.
Convert AQI category variable from numerical to categorical
delete 24 hour mid point conc column and add now cast conc column*;

* deleting 985 raw conc values*;

data hcmc_2016_12;
set hcmc_2016_12;
if raw_conc_ >900 then  raw_conc_=.;
if qc_name = 'Inval' then qc_name = '';
run;

* back filling values for raw_conc and treating qc_name*;
* sorting to make sure the values are in order*;

proc sort data=hcmc_2016_12;
by date__lt_;
run;

DATA hcmc_2016_12;
	SET hcmc_2016_12;
	RETAIN _Raw_Conc_;

	IF NOT MISSING(Raw_Conc_) THEN
		_Raw_Conc_=Raw_Conc_;
	ELSE
		Raw_Conc_=_Raw_Conc_;
	DROP _Raw_Conc_;
	RETAIN _YEAR;
	
	if qc_name ='' then qc_name ='Valid';
	run;
********************************************************************************************************************************;
	
* Deleting 24 hour mid point conc column and calculate now cast conc column*;

data hcmc_2016_12;
set hcmc_2016_12 (Drop= _24_hr__Midpoint_Avg__Conc_);
run;

data hcmc_2016_12;
	array conc{0:11} _temporary_;
	set hcmc_2016_12 ;
	by Date__LT_;
	conc{11}=conc{10};
	conc{10}=conc{9};
	conc{9}=conc{8};
	conc{8}=conc{7};
	conc{7}=conc{6};
	conc{6}=conc{5};
	conc{5}=conc{4};
	conc{4}=conc{3};
	conc{3}=conc{2};
	conc{2}=conc{1};
	conc{1}=conc{0};
	conc{0}=Raw_conc_;
	c_min=min(of conc{*});
	c_max=max(of conc{*});
	weight_factor=c_min/c_max;

	if weight_factor<0.5 then
		weight_factor=0.5;
	sum_product=0;
	weight_sum=0;

	do i=0 to 11;

		if conc{i}='.' then
			continue;
		sum_product=sum_product+conc{i}*(weight_factor**(i));
		weight_sum=weight_sum+(weight_factor**(i));
	end;
	NowCast_Conc_=sum_product/weight_sum;
	NowCast_Conc_=Round(NowCast_Conc_, 0.1);
	drop c_max c_min i weight_factor weight_sum sum_product;

run;
	
* Re calculating AQI values*;

DATA hcmc_2016_12;
	SET hcmc_2016_12;
	if (NowCast_Conc_>=0 and NowCast_Conc_<12.1) then
		AQI=round(((NowCast_Conc_-0)/(12))*(50-0)+0);

	if (NowCast_Conc_>=12.1 and NowCast_Conc_<35.5) then
		AQI=round(((NowCast_Conc_-12.1)/(35.4-12.1))*(100-51)+51);

	if (NowCast_Conc_>=35.5 and NowCast_Conc_<55.5) then
		AQI=round(((NowCast_Conc_-35.5)/(55.4-35.5))*(150-101)+101);

	if (NowCast_Conc_>=55.5 and NowCast_Conc_<150.5) then
		AQI=round(((NowCast_Conc_-55.5)/(150.4-55.5))*(200-151)+151);

	if (NowCast_Conc_>=150.5 and NowCast_Conc_<250.5) then
		AQI=round(((NowCast_Conc_-150.5)/(250.4-150.5))*(300-201)+201);

	if (NowCast_Conc_>=250.5 and NowCast_Conc_<350.5) then
		AQI=round(((NowCast_Conc_-250.5)/(350.4-250.5))*(400-301)+301);

	if (NowCast_Conc_>=350.5 and NowCast_Conc_<500.5) then
		AQI=round(((NowCast_Conc_-350.5)/(500.4-350.5))*(500-401)+0);
run;		
		
/* creating a new column Aqi_Category_Corrected*/;

DATA hcmc_2016_12;
	SET hcmc_2016_12;

	LENGTH AQI_Category_Corrected $30.;

	IF AQI_CATEGORY=1 THEN
		AQI_CATEGORY_CORRECTED='Good';

	IF AQI_CATEGORY=2 THEN
		AQI_CATEGORY_CORRECTED='Moderate';

	IF AQI_CATEGORY=3 THEN
		AQI_CATEGORY_CORRECTED='Unhealthy for Sensitive Groups';

	IF AQI_CATEGORY=4 THEN
		AQI_CATEGORY_CORRECTED='Unhealthy';

	IF AQI_CATEGORY=5 THEN
		AQI_CATEGORY_CORRECTED='Very Unhealthy';

	IF AQI_CATEGORY=6 THEN
		AQI_CATEGORY_CORRECTED='Hazarodus';
RUN;

/* Now removing old 'AQI_Category' column and renaming 'AQI_Category_Corrected' to 'AQI_Category'*/
DATA hcmc_2016_12 (DROP=AQI_Category);
	SET hcmc_2016_12;
RUN;

DATA hcmc_2016_12 (RENAME=(AQI_Category_Corrected=AQI_Category));
	SET hcmc_2016_12;

run;

********************************************************************************************************************************;

* Now all the datasets have same 14 variables. Lets merge the concatenate the datasets for further analysis*;
* The new dataset will have observations in the following order : 2016_12, 2017_12, 2018_12, 2019_12, 2020_12, 2021_02;

data HCMC_merged;
set hcmc_2016_12 hcmc_2017_12 hcmc_2018_12 hcmc_2019_12 hcmc_2020_12 hcmc_2021_02;
by date__lt_;
run;
********************************************************************************************************************************;

* Exploring the merged dataset*;

proc contents data=HCMC_merged;
run;

* Exploring all the numeric variables with proc means procedure*;

PROC MEANS DATA=HCMC_merged n nmiss mean median min max skewness kurtosis std mode range;
VAR _numeric_;
RUN;

*only customerId variable has missing values*;

* Exploring Character variables*;

proc freq data=HCMC_merged order=freq ;
table _char_ ;
run;

********************************************************************************************************************************;

*Inferences:
The dataset has no missing values in numeric as well as character variables
AQI, raw conc and now cast conc has -999 values
QC_name has 105 inval, 61 missi values
AQI_Category has 147 N/A values*;

********************************************************************************************************************************;

* Visualizing where we have 'Invalid' and 'Missing' values in QC_Name variable;

ods graphics / reset width=6.4in height=4.8in imagemap;

proc sort data=WORK.HCMC_MERGED out=_BarChartTaskData;
	by Year;
run;

proc sgplot data=_BarChartTaskData;
	by Year;
	vbar QC_Name / group=Month groupdisplay=cluster;
	yaxis grid;
run;

ods graphics / reset;

proc datasets library=WORK noprint;
	delete _BarChartTaskData;
	run;
********************************************************************************************************************************;

* COnverting all invalid values to nan to treat*;
data hcmc_merged;
set hcmc_merged;
if Qc_name = "Inval" or qc_name = 'Missi' then
raw_conc_ = .;

if Qc_name = "Inval" or qc_name = 'Missi' then
NowCast_Conc_ = .;

if Qc_name = "Inval" or qc_name = 'Missi' then
AQI = .;

if Qc_name = "Inval" or qc_name = 'Missi' then
AQI_Category= '';

if AQI_Category = 'N/A' then AQI_Category = '';
run;

proc sort data=hcmc_merged;
by date__lt_;
run;

DATA hcmc_merged;
	SET hcmc_merged;
	RETAIN _Raw_Conc_;

	IF NOT MISSING(Raw_Conc_) THEN
		_Raw_Conc_=Raw_Conc_;
	ELSE
		Raw_Conc_=_Raw_Conc_;
	DROP _Raw_Conc_;
	RETAIN _YEAR;
	run;

data hcmc_merged;
	array conc{0:11} _temporary_;
	set hcmc_merged ;
	by Date__LT_;
	conc{11}=conc{10};
	conc{10}=conc{9};
	conc{9}=conc{8};
	conc{8}=conc{7};
	conc{7}=conc{6};
	conc{6}=conc{5};
	conc{5}=conc{4};
	conc{4}=conc{3};
	conc{3}=conc{2};
	conc{2}=conc{1};
	conc{1}=conc{0};
	conc{0}=Raw_conc_;
	c_min=min(of conc{*});
	c_max=max(of conc{*});
	weight_factor=c_min/c_max;

	if weight_factor<0.5 then
		weight_factor=0.5;
	sum_product=0;
	weight_sum=0;

	do i=0 to 11;

		if conc{i}='.' then
			continue;
		sum_product=sum_product+conc{i}*(weight_factor**(i));
		weight_sum=weight_sum+(weight_factor**(i));
	end;
	NowCast_Conc_=sum_product/weight_sum;
	NowCast_Conc_=Round(NowCast_Conc_, 0.1);
	drop c_max c_min i weight_factor weight_sum sum_product;

run;
	
* Re calculating AQI values*;

DATA hcmc_merged;
	SET hcmc_merged;
	if (NowCast_Conc_>=0 and NowCast_Conc_<12.1) then
		AQI=round(((NowCast_Conc_-0)/(12))*(50-0)+0);

	if (NowCast_Conc_>=12.1 and NowCast_Conc_<35.5) then
		AQI=round(((NowCast_Conc_-12.1)/(35.4-12.1))*(100-51)+51);

	if (NowCast_Conc_>=35.5 and NowCast_Conc_<55.5) then
		AQI=round(((NowCast_Conc_-35.5)/(55.4-35.5))*(150-101)+101);

	if (NowCast_Conc_>=55.5 and NowCast_Conc_<150.5) then
		AQI=round(((NowCast_Conc_-55.5)/(150.4-55.5))*(200-151)+151);

	if (NowCast_Conc_>=150.5 and NowCast_Conc_<250.5) then
		AQI=round(((NowCast_Conc_-150.5)/(250.4-150.5))*(300-201)+201);

	if (NowCast_Conc_>=250.5 and NowCast_Conc_<350.5) then
		AQI=round(((NowCast_Conc_-250.5)/(350.4-250.5))*(400-301)+301);

	if (NowCast_Conc_>=350.5 and NowCast_Conc_<500.5) then
		AQI=round(((NowCast_Conc_-350.5)/(500.4-350.5))*(500-401)+0);
run;		
		
/* creating a new column Aqi_Category_Corrected*/;

DATA hcmc_merged;
	SET hcmc_merged;

	IF AQI_CATEGORY='' THEN
		DO;

			IF AQI >=0 AND AQI <=50 THEN
				AQI_CATEGORY='Good';

			IF AQI >=51 AND AQI <=100 THEN
				AQI_CATEGORY='Moderate';

			IF AQI >=101 AND AQI <=150 THEN
				AQI_CATEGORY='Unhealthy for Sensitive Groups';

			IF AQI >=151 AND AQI <=200 THEN
				AQI_CATEGORY='Unhealthy';

			IF AQI >=201 AND AQI <=300 THEN
				AQI_CATEGORY='Very Unhealthy';

			IF AQI >=301 AND AQI <=500 THEN
				AQI_CATEGORY='Hazardous';
		END;		
RUN;
********************************************************************************************************************************;

* Since all wrong values have been treated, we can change 'Inval' and 'Missi' in QC_Name to 'Valid';

data hcmc_merged;
set hcmc_merged;

if qc_name='Inval' or qc_name ='Missi' then qc_Name = 'Valid';
run;
********************************************************************************************************************************;
********************************************************************************************************************************;
											* DESCRIPTIVE STATISTICS*;
********************************************************************************************************************************;
********************************************************************************************************************************;

* 1. Lets determine the normality of the variables raw_conc, now cast conc and aqi using histograms, univariate analysis
and skewness and kurtosis parameters*;

proc univariate data=hcmc_merged;
by year;
var raw_conc_ nowcast_conc_ aqi;
histogram raw_conc_ nowcast_conc_ aqi;
run;

********************************************************************************************************************************;
* box plot of raw_conc grouped by year*;

proc sgplot data=WORK.HCMC_MERGED;
	title height=14pt "Box plot of 'Raw_Conc_' for years 2016 - 2021";
	vbox Raw_Conc_ / group=Year;
	yaxis grid;
run;

********************************************************************************************************************************;
* box plot of now_Cast_conc grouped by year*;

proc sgplot data=WORK.HCMC_MERGED;
	title height=14pt "Box plot of 'Now_Cast_Conc_' for years 2016 - 2021";
	vbox NowCast_Conc_ / group=Year;
	yaxis grid;
run;

********************************************************************************************************************************;
* box plot of AQI grouped by year*;

proc sgplot data=WORK.HCMC_MERGED;
	title height=14pt "Box plot of 'AQI' for years 2016 - 2021";
	vbox AQI / group=Year;
	yaxis grid;
run;

********************************************************************************************************************************;
* Bar chart to determine the average air quality in the month of december for the years 2016-2021*;

proc sort data=WORK.HCMC_MERGED out=_BarChartTaskData;
	by date__lt_;
run;

proc sgplot data=_BarChartTaskData;
	by Year;
	title height=14pt "Average air quality in the month of Dec for the";
	vbar AQI_Category / datalabel;
	yaxis grid;
run;

ods graphics / reset;
title;

proc datasets library=WORK noprint;
	delete _BarChartTaskData;
	run;
	
********************************************************************************************************************************;
* Bar chart to determine the average AQI in each AQI category in the month of December of the years 2016 - 2021*;	

proc sort data=WORK.HCMC_MERGED out=_BarChartTaskData;
	by date__lt_;
run;

proc sgplot data=_BarChartTaskData;
	by Year;
	title height=14pt "Average AQI in each AQI Category for the";
	vbar AQI_Category / response=AQI fillattrs=(color=CXf09d6d) fillType=gradient 
		stat=mean;
	yaxis grid;
run;

ods graphics / reset;
title;

proc datasets library=WORK noprint;
	delete _BarChartTaskData;
	run;	
********************************************************************************************************************************;
* Average PM 2.5 pollutant concentration in the month of December of the years 2016 - 2021*;
	
proc sgplot data=WORK.HCMC_MERGED;
	title height=14pt "Average PM 2.5 Pollutant concentration in Dec Month";
	vbar Year / response=Raw_Conc_ fillattrs=(color=CX7ce2ab) fillType=gradient 
		stat=mean;
	yaxis grid;
run;
	
********************************************************************************************************************************;
********************************************************************************************************************************;
									* CORRELATION ANALYSIS* Or * MULTIVARIATE ANALYSIS *
********************************************************************************************************************************;
********************************************************************************************************************************;

* Correlation analysis of the continuous variables - Raw_Conc_ NowCast_Conc_ AQI*;

proc sort data=WORK.HCMC_MERGED out=Work.SortTempTableSorted;
	by date__lt_;
run;

proc corr data=Work.SortTempTableSorted pearson spearman 
		plots=matrix(histogram);
	var AQI Raw_Conc_ NowCast_Conc_;
	by Year;
run;

proc delete data=Work.SortTempTableSorted;
run;

********************************************************************************************************************************;
********************************************************************************************************************************;
									* ANALYSIS WITH EXTERNAL DATASETS*
********************************************************************************************************************************;
********************************************************************************************************************************;
* Loading external datasets*

/*1st file:*/;
PROC IMPORT OUT=HCMC_allweather_2016_12
		DATAFILE='/folders/myfolders/DANA-Group-6-team-project/External data CSV files/HCMC all weather Dec_2016.csv' 
		DBMS=CSV REPLACE; GETNAMES=YES;	DATAROW=2;
RUN;

/*2nd file:*/
PROC IMPORT OUT=HCMC_allweather_2017_12
		DATAFILE='/folders/myfolders/DANA-Group-6-team-project/External data CSV files/HCMC all weather Dec_2017.csv' 
		DBMS=CSV REPLACE; GETNAMES=YES;	DATAROW=2;
RUN;

/*3rd file:*/
PROC IMPORT OUT=HCMC_allweather_2018_12
		DATAFILE='/folders/myfolders/DANA-Group-6-team-project/External data CSV files/HCMC all weather Dec_2018.csv' 
		DBMS=CSV REPLACE; GETNAMES=YES;	DATAROW=2;
RUN;

/*4th file:*/
PROC IMPORT OUT=HCMC_allweather_2019_12
		DATAFILE='/folders/myfolders/DANA-Group-6-team-project/External data CSV files/HCMC all weather Dec_2019.csv' 
		DBMS=CSV REPLACE; GETNAMES=YES;	DATAROW=2;
RUN;

/*5th file:*/
PROC IMPORT OUT=HCMC_allweather_2020_12
		DATAFILE='/folders/myfolders/DANA-Group-6-team-project/External data CSV files/HCMC all weather Dec_2020.csv' 
		DBMS=CSV REPLACE; GETNAMES=YES;	DATAROW=2;
RUN;

/*6th file:*/
PROC IMPORT OUT=HCMC_allweather_2021_02
		DATAFILE='/folders/myfolders/DANA-Group-6-team-project/External data CSV files/HCMC all weather Feb_2021.csv' 
		DBMS=CSV REPLACE; GETNAMES=YES;	DATAROW=2;
RUN;

********************************************************************************************************************************;
* All the datasets have same set of variables. Lets concatenate them into a single dataset and then explore for data quality*;

data HCMC_ALLWEATHER_MERGED;
set hcmc_allweather_2016_12 
hcmc_allweather_2017_12 
hcmc_allweather_2018_12 
hcmc_allweather_2019_12 
hcmc_allweather_2020_12
hcmc_allweather_2021_02;
by year;
run;

********************************************************************************************************************************;

* exploring the contents of the all weather data*;

proc contents data=hcmc_allweather_merged;
run;

* Exploring all the numeric variables with proc means procedure*;

PROC MEANS DATA=hcmc_allweather_merged n nmiss mean median min max skewness kurtosis std mode range;
VAR _numeric_;
RUN;

* we infer that the columns Atm_Pressure_sealevel and Total_rainfall_snowmelt are interpreted as character variables
However these columns are 100% empty. Therefore delete those columns*;

data hcmc_allweather_merged;
set hcmc_allweather_merged (drop= Atm_Pressure_sealevel Total_rainfall_snowmelt );
run;

* all variables are numeric. No missing values and out of range values*

The external dataset has daily values and not hourly*;

********************************************************************************************************************************;
* Make the previous air pollution data of HCMC city as daily by taking average values of day*;

PROC SQL; 
CREATE TABLE WORK.hcmc_merged_daily 
AS 
SELECT 
DISTINCT HCMC_MERGED.Site, HCMC_MERGED.Parameter, HCMC_MERGED.Year, HCMC_MERGED.Month, HCMC_MERGED.Day, AVG(HCMC_MERGED.AQI) 
AS AQI, AVG(HCMC_MERGED.Raw_Conc_) 
AS Raw_Conc_, HCMC_MERGED.Conc__Unit, HCMC_MERGED.Duration, HCMC_MERGED.QC_Name, AVG(HCMC_MERGED.NowCast_Conc_) 
AS NowCast_Conc_, MIN(HCMC_MERGED.AQI_Category) 
AS AQI_Category 
FROM WORK.HCMC_MERGED HCMC_MERGED 
GROUP BY HCMC_MERGED.Year, HCMC_MERGED.Month, HCMC_MERGED.Day; 
QUIT;

* Merging the pollution dataset with the new all weather dataset*;

proc sql;
create table HCMC_FULL_DATA as
select A.*, B.Avg_Temp, B.Max_Temp, B.Min_Temp, B.Avg_Relative_Humidity, B.Avg_Visibility, B.Avg_Windspeed, B.Max_Windspeed
from hcmc_merged_daily A, hcmc_allweather_merged B
where A.year=B.year and A.month=B.month and A.day=B.day
group by A.Year, A.Month, A.Day;
quit;

* Rounding off the high decimal values*;

data hcmc_full_data;
set hcmc_full_data;
aqi = round(aqi,1);
raw_conc_ = round(raw_conc_ ,0.1);
nowcast_conc_ = round(nowcast_conc_, 0.1);
run;

********************************************************************************************************************************;
********************************************************************************************************************************;
									* CORRELATION ANALYSIS WITH EXTERNAL DATASETS*
********************************************************************************************************************************;
********************************************************************************************************************************;
* 2.	Use external dataset and determine the correlation of temperature, atmospheric pressure, humidity, visibility,
and wind speed with the air pollution of HCMC city. Explain how air pollution impacts all other weather parameters.;

*Performing correlatoin analysis on the whole dataset to answer this question*;

proc sort data=WORK.HCMC_FULL_DATA out=Work.SortTempTableSorted;
	by Year;
run;

proc corr data=Work.SortTempTableSorted pearson spearman outp=work.Corr_stats 
		plots=matrix(histogram);
	var Raw_Conc_ AQI NowCast_Conc_ Avg_Temp Avg_Relative_Humidity Avg_Visibility 
		Avg_Windspeed;
	by Year;
run;

proc delete data=Work.SortTempTableSorted;
run;

********************************************************************************************************************************;
* Summarizing the above analysis with the help of a bar chart grouped by year*;

proc sort data=WORK.CORR_STATS out=_BarChartTaskData;
	by Year;
run;

proc sgplot data=_BarChartTaskData;
	by Year;
	title height=14pt 
		"Summary of Impact of PM 2.5 pollutant on weather parameters";
	hbar _NAME_ / response=Raw_Conc_ fillattrs=(color=CXf8f68d) datalabel 
		stat=mean;
	xaxis grid;
run;

ods graphics / reset;
title;

proc datasets library=WORK noprint;
	delete _BarChartTaskData;
	run;
********************************************************************************************************************************;
* General Summary of the correlation analysis with the help of a bar chart*;

proc sgplot data=WORK.CORR_STATS;
	title height=14pt "Average Impact of PM 2.5 pollutant on weather parameters";
	hbar _NAME_ / response=Raw_Conc_ fillattrs=(color=CX8df8f8) datalabel 
		stat=mean;
	xaxis grid;
run;

* P.S - Similar bar chart for AQI was also plotted and the correlation values of the weather parameters wrt to AQI
 were same as they are wrt raw conc
********************************************************************************************************************************;

* The hypotheses we obtain from the above correlation analysis with respect to PM 2.5 pollutant are

1. There is negative correlation of about 50% between wind speed and the concentration of PM 2.5 pollutant in HCMC city
	( Wind speed is less when the PM 2.5 conc is more in the air) 

2. There is negative correlation of about 45% between Visibility and the concentration of PM 2.5 pollutant in HCMC city
	( Visibility is less when the PM 2.5 conc is more in the air)

3. The average temerature in HCMC city is not correlated or impacted by the concentration of PM 2.5 pollutant in the atmosphere.

4. The average Humidity in HCMC city is not correlated or impacted by the concentration of PM 2.5 pollutant in the atmosphere.

********************************************************************************************************************************;
********************************************************************************************************************************;
												* HYPOTHESES TESTING*
********************************************************************************************************************************;
********************************************************************************************************************************;

* Points to Note:
- If p value (alpha) in t test less than 0.05, then reject null hypothesis. and accept alternate hypothesis
- Sides = 2 represents Two tailed t test
- higher the T value (+ve or -ve), represents strong evidence against null hypothesis
- closer to 0 T value represents weak evidence against null hypothesis
- Based on decision rule and comparing the t value with T table, reject null hypothesis if t value > 1.96 or <-1.96

********************************************************************************************************************************;

* 1. H0 = The population correlation coefficient between Raw_Conc and AQI for the population (represented by 'rho') is 0.
			Meaning - There is no correlation between raw_conc and AQI in population dataset.			
	 Ha = The population correlation coefficient 'rho' between Raw_Conc and AQI is not 0.
	 		Meaning - There is correlation between raw_conc and AQI in population dataset.
	 		( sample correlation coefficient 'r' = 98.82%)*;
	 		
proc ttest data= hcmc_full_data h0=0 sides=2 alpha=0.05;
paired raw_conc_* AQI;
run;

* Result:
- P value less than 0.05 means that, we ACCEPT ALTERNATE HYPOTHESIS, we have correlation between raw_conc and AQI values in population
- t value = -78.84 implies we have very strong evidence in rejecting null hypothesis.
- In population, increase or decrease in magnitude of raw conc, increases or decreases AQI
********************************************************************************************************************************;

* 2. H0 = The population correlation coefficient between Raw_Conc and NowCast_Conc_ for the population (represented by 'rho') is 0.
			Meaning - There is no correlation between raw_conc and NowCast_Conc_ in population dataset.			
	 Ha = The population correlation coefficient 'rho' between Raw_Conc and NowCast_Conc_ is not 0.
	 		Meaning - There is correlation between raw_conc and NowCast_Conc_ in population dataset.
	 		( sample correlation coefficient 'r' = 99.8%)*;

proc ttest data= hcmc_full_data h0=0 sides=2 alpha=0.05;
paired raw_conc_* NowCast_Conc_;
run;

* Result:
- P value is 0.6072 = 60.72% means that, we accept null hypothesis, we do not have correlation between raw_conc and Now cast values in population
- t value = -0.52 implies we have very weak evidence in rejecting null hypothesis.
- In population, increase or decrease in magnitude of raw conc does not increase or decrease Now cast values
********************************************************************************************************************************;

* 3. H0 = The population correlation coefficient between Raw_Conc and Temperature for the population (represented by 'rho') is 0.
			Meaning - There is no correlation between raw_conc and Temperature in population dataset.			
	 Ha = The population correlation coefficient 'rho' between Raw_Conc and Temperature is not 0.
	 		Meaning - There is correlation between raw_conc and Temperature in population dataset.
	 		( sample correlation coefficient 'r' = 0.15%)*;

proc ttest data= hcmc_full_data h0=0 sides=2 alpha=0.05;
paired raw_conc_* Avg_Temp;
run;

* Result:
- P value less than 0.05 means that, we ACCEPT ALTERNATE HYPOTHESIS, we have correlation between raw_conc and Temperature values in population
- t value = 8.62 implies we have strong evidence in rejecting null hypothesis.
- In population, increase in temperature is affected with increase in raw_conc (PM 2.5 pollutant)
********************************************************************************************************************************;

* 4. H0 = The population correlation coefficient between Raw_Conc and Humidity for the population (represented by 'rho') is 0.
			Meaning - There is no correlation between raw_conc and Humidity in population dataset.			
	 Ha = The population correlation coefficient 'rho' between Raw_Conc and Humidity is not 0.
	 		Meaning - There is correlation between raw_conc and Humidity in population dataset.
	 		( sample correlation coefficient 'r' = 0.27%)*;

proc ttest data= hcmc_full_data h0=0 sides=2 alpha=0.05;
paired raw_conc_* Avg_relative_humidity;
run;

* Result:
- P value less than 0.05 means that, we ACCEPT ALTERNATE HYPOTHESIS, we have correlation between raw_conc and Humidity values in population
- t value = -41.77 implies we have very strong evidence in rejecting null hypothesis.
- In population, increase in Humidity is affected with increase in raw_conc (PM 2.5 pollutant)
********************************************************************************************************************************;

* 5. H0 = The population correlation coefficient between Raw_Conc and Visibility for the population (represented by 'rho') is 0.
			Meaning - There is no correlation between raw_conc and Visibility in population dataset.			
	 Ha = The population correlation coefficient 'rho' between Raw_Conc and Visibility is not 0.
	 		Meaning - There is correlation between raw_conc and Visibility in population dataset.
	 		( sample correlation coefficient 'r' = -47%)*;

proc ttest data= hcmc_full_data h0=0 sides=2 alpha=0.05;
paired raw_conc_* Avg_visibility;
run;

* Result:
- P value less than 0.05 means that, we ACCEPT ALTERNATE HYPOTHESIS, we have correlation between raw_conc and Visibility values in population
- t value = 30.22 implies we have very strong evidence in rejecting null hypothesis.
- In population, visibility is inversely correlated with PM 2.5 pollutant(because r=-47%). That is, higher pollution implies lesser visibility
********************************************************************************************************************************;

* 6. H0 = The population correlation coefficient between Raw_Conc and WindSpeed for the population (represented by 'rho') is 0.
			Meaning - There is no correlation between raw_conc and WindSpeed in population dataset.			
	 Ha = The population correlation coefficient 'rho' between Raw_Conc and WindSpeed is not 0.
	 		Meaning - There is correlation between raw_conc and WindSpeed in population dataset.
	 		( sample correlation coefficient 'r' = -50%)*;

proc ttest data= hcmc_full_data h0=0 sides=2 alpha=0.05;
paired raw_conc_* Avg_windspeed;
run;

* Result:
- P value less than 0.05 means that, we ACCEPT ALTERNATE HYPOTHESIS, we have correlation between raw_conc and WindSpeed values in population
- t value = 29.22 implies we have very strong evidence in rejecting null hypothesis.
- In population, WindSpeed is inversely correlated with PM 2.5 pollutant(because r=-50%). That is, higher pollution implies lesser WindSpeed
********************************************************************************************************************************;

********************************************************************************************************************************;
********************************************************************************************************************************;
												* LINEAR MODEL*
********************************************************************************************************************************;
********************************************************************************************************************************;

* Dependent variable: AQI. independent features selected by modelling all features into linear model and selecting
most affecting features Raw_Conc_ NowCast_Conc_ Avg_Temp Avg_Visibility Avg_Windspeed:; 

proc reg data=WORK.HCMC_FULL_DATA alpha=0.05 plots(only)=(diagnostics residuals 
		observedbypredicted) outest=linear_est;
AQI_hat: model AQI=Raw_Conc_ NowCast_Conc_ Avg_Temp Avg_Visibility Avg_Windspeed /;
	run;
quit;

proc print data= linear_est;
run;

********************************************************************************************************************************;
********************************************************************************************************************************;
												* ARIMA MODEL*
********************************************************************************************************************************;
********************************************************************************************************************************;

* We have AQI values till february 23rd 2021 in our dataset. What will be the AQI values from 24th feb to 28th feb ?

* Time series exploration to determine P, D, Q values:;

data hcmc_full_data_arima;
set hcmc_full_data;
date=mdy(month,day,year);
  format date mmddyy10.;
run;


* P = 1 (from PACF plot)
  D = 1 (seasonality observed)
  Q = 3 (from ACF plot);


* ARIMA MODEL*;

proc sort data=WORK.HCMC_FULL_DATA_ARIMA out=Work.preProcessedData;
	by date;
run;

proc arima data=Work.preProcessedData plots
     (only)=(series(acf corr crosscorr pacf) residual(corr normal) 
		forecast(forecast) ) out=work.arima_forecast;
	identify var=AQI(1);
	estimate p=(1) q=(1 2 3) method=ML;
	forecast lead=5 back=0 alpha=0.05 id=date interval=day;
	outlier;
	run;
quit;

proc delete data=Work.preProcessedData;
run;


*********************************************   THE END   **********************************************************************;
********************************************************************************************************************************;








