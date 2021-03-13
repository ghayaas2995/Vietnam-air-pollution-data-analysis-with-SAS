*Importing the HCMC datasets into sas*;

/*1st file:*/
PROC IMPORT OUT=HCMC_2016_12
		DATAFILE='/folders/myfolders/DANA-Group-6-team-project/HoChiMinhCity_PM2.5_2016_12_MTD.csv' 
		DBMS=CSV REPLACE;
	GETNAMES=YES;
	DATAROW=2;
RUN;

/*2nd file:*/
PROC IMPORT OUT=HCMC_2017_12
		DATAFILE='/folders/myfolders/DANA-Group-6-team-project/HoChiMinhCity_PM2.5_2017_12_MTD.csv' 
		DBMS=CSV REPLACE;
	GETNAMES=YES;
	DATAROW=2;
RUN;

/*3rd file:*/
PROC IMPORT OUT=HCMC_2018_12
		DATAFILE='/folders/myfolders/DANA-Group-6-team-project/HoChiMinhCity_PM2.5_2018_12_MTD.csv' 
		DBMS=CSV REPLACE;
	GETNAMES=YES;
	DATAROW=2;
RUN;

/*4th file:*/
PROC IMPORT OUT=HCMC_2019_12
		DATAFILE='/folders/myfolders/DANA-Group-6-team-project/HoChiMinhCity_PM2.5_2019_12_MTD.csv' 
		DBMS=CSV REPLACE;
	GETNAMES=YES;
	DATAROW=2;
RUN;

/*5th file:*/
PROC IMPORT OUT=HCMC_2020_12
		DATAFILE='/folders/myfolders/DANA-Group-6-team-project/HoChiMinhCity_PM2.5_2020_12_MTD.csv' 
		DBMS=CSV REPLACE;
	GETNAMES=YES;
	DATAROW=2;
RUN;

/*6th file:*/
PROC IMPORT OUT=HCMC_2021_02
		DATAFILE='/folders/myfolders/DANA-Group-6-team-project/HoChiMinhCity_PM2.5_2021_02_MTD.csv' 
		DBMS=CSV REPLACE;
	GETNAMES=YES;
	DATAROW=2;
RUN;

********************************************************************************************************************************;
* CLEANING 2016 MONTH DATASET*;

* lets first explore contents and means of this dataset*;

proc contents data=HCMC_2016_12;
run;

* Exploring all the numeric variables with proc means procedure*;

PROC MEANS DATA=HCMC_2016_12 n nmiss mean median min max skewness kurtosis std mode range;
VAR _numeric_;
RUN;

*only customerId variable has missing values*;

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
	
* Converting AQI category variable from numerical to categorical*;

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
RUN;
	
* Deleting 24 hour mid point conc column and add now cast conc column*;

data hcmc_2016_12;
set hcmc_2016_12 (Drop= _24_hr__Midpoint_Avg__Conc_);
run;

proc sql;
create table calc_minmax as 
select Date__LT_,raw_conc_,
(select max(Raw_Conc_) from hcmc_2016_12 as b
	where intnx('hour', a.Date__LT_, -11, 'b') le b.Date__LT_ le a.Date__LT_) as max_conc,
(select min(Raw_Conc_) from hcmc_2016_12 as b
	where intnx('hour', a.Date__LT_, -11, 'b') le b.Date__LT_ le a.Date__LT_) as min_conc
	from hcmc_2016_12 as a;
quit;

proc sql;
select *, case
	when min_conc/max_conc < 0.5 then 0.5
	else min_conc/max_conc
	end as weight 
	from calc_minmax;
quit;

data calc_minmax1;
array raw_conc_{0:-11} _temporary_;
set calc_minmax1;
by Date__lt_;
do i 0 to 11;
	sum_product = sum_product+(raw_conc{i}*(weight**{i}));
run;

********************************************************************************************************************************;



































































