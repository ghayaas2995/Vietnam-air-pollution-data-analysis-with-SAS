*import data HoChiMinhCity_PM2.5_2016_12_MTD.csv;
proc import datafile='/folders/myfolders/DANA-Group-6-team-project/HoChiMinhCity_PM2.5_2016_12_MTD.csv'
	dbms=csv
	out=work.HCMC_2016_12 replace;
	GETNAMES=YES;
    DATAROW=2;
run;

/* data work.HCMC_2016_12; */
/* 	format QC_Name $7.; */
/* 	set work.HCMC_2016_12; */
/* 	rename _24_hr__Midpoint_Avg__Conc_ = Midpoint_Avg_Conc_24hr */
/* 	Date__LT_ = Date_LT */
/* 	Raw_Conc_ = Raw_Conc_ */
/* 	Conc__Unit = Conc_Unit; */
/* 	if QC_Name = 'Inval' then QC_Name='Invalid'; */
/* run; */

* deleting 985 raw conc values*;

data HCMC_2016_12;
	set HCMC_2016_12;
	if Raw_Conc_ >900 then  Raw_Conc_=.;
run;

* back filling values for Raw_Conc_;

proc sort data=HCMC_2016_12;
	by Date__LT_;
run;

DATA HCMC_2016_12;
	SET HCMC_2016_12;
	RETAIN _Raw_Conc_;

	IF NOT MISSING(Raw_Conc_) THEN
		_Raw_Conc_=Raw_Conc_;
	ELSE
		Raw_Conc_=_Raw_Conc_;
	DROP _Raw_Conc_;
	RETAIN _YEAR;
run;

* drop 24_hr_midpoint_column;

data HCMC_2016_12; 
	set HCMC_2016_12; 
	drop _24_hr__Midpoint_Avg__Conc_; 
run; 

*Calculating NowCast value;

data HCMC_2016_12;
	array conc{0:11} _temporary_;
	set HCMC_2016_12 ;
	by  Date__LT_;
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
	conc{0}=Raw_Conc_;
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
	
	Nowcast_conc_=sum_product/weight_sum;
	Nowcast_conc_=Round(Nowcast_conc_, 0.1);
	drop c_max c_min i weight_factor weight_sum sum_product;

run;

*Recalculating AQI as AQI_new;

DATA HCMC_2016_12;
	SET HCMC_2016_12;
	if (Nowcast_conc_>=0 and Nowcast_conc_<12.1) then
		AQI=round(((Nowcast_conc_-0)/(12))*(50-0)+0);

	if (Nowcast_conc_>=12.1 and Nowcast_conc_<35.5) then
		AQI=round(((Nowcast_conc_-12.1)/(35.4-12.1))*(100-51)+51);

	if (Nowcast_conc_>=35.5 and Nowcast_conc_<55.5) then
		AQI=round(((Nowcast_conc_-35.5)/(55.4-35.5))*(150-101)+101);

	if (Nowcast_conc_>=55.5 and Nowcast_conc_<150.5) then
		AQI=round(((Nowcast_conc_-55.5)/(150.4-55.5))*(200-151)+151);

	if (Nowcast_conc_>=150.5 and Nowcast_conc_<250.5) then
		AQI=round(((Nowcast_conc_-150.5)/(250.4-150.5))*(300-201)+201);

	if (Nowcast_conc_>=250.5 and Nowcast_conc_<350.5) then
		AQI=round(((Nowcast_conc_-250.5)/(350.4-250.5))*(400-301)+301);

	if (Nowcast_conc_>=350.5 and Nowcast_conc_<500.5) then
		AQI=round(((Nowcast_conc_-350.5)/(500.4-350.5))*(500-401)+0);
run;

*AQI Category to Categorical variable;
DATA HCMC_2016_12;
	SET HCMC_2016_12;

	LENGTH AQI_Category_Corrected $30.;

	IF AQI_CATEGORY=1 THEN AQI_CATEGORY_CORRECTED='Good';

	IF AQI_CATEGORY=2 THEN AQI_CATEGORY_CORRECTED='Moderate';

	IF AQI_CATEGORY=3 THEN AQI_CATEGORY_CORRECTED='Unhealthy for Sensitive Groups';

	IF AQI_CATEGORY=4 THEN AQI_CATEGORY_CORRECTED='Unhealthy';

	IF AQI_CATEGORY=5 THEN AQI_CATEGORY_CORRECTED='Very Unhealthy';

	IF AQI_CATEGORY=6 THEN AQI_CATEGORY_CORRECTED='Hazarodus';
RUN;

DATA HCMC_2016_12 (DROP=AQI_Category);
	SET HCMC_2016_12;
RUN;

DATA HCMC_2016_12 (RENAME=(AQI_Category_Corrected=AQI_Category));
	SET HCMC_2016_12;

run;
*************************************************************************************************************;

*Merged dataset;

PROC IMPORT OUT=HCMC_2017_12
		DATAFILE='/folders/myfolders/DANA-Group-6-team-project/HoChiMinhCity_PM2.5_2017_12_MTD.csv' 
		DBMS=CSV REPLACE; GETNAMES=YES;	DATAROW=2;
RUN;

PROC IMPORT OUT=HCMC_2018_12
		DATAFILE='/folders/myfolders/DANA-Group-6-team-project/HoChiMinhCity_PM2.5_2018_12_MTD.csv' 
		DBMS=CSV REPLACE; GETNAMES=YES;	DATAROW=2;
RUN;

PROC IMPORT OUT=HCMC_2019_12
		DATAFILE='/folders/myfolders/DANA-Group-6-team-project/HoChiMinhCity_PM2.5_2019_12_MTD.csv' 
		DBMS=CSV REPLACE; GETNAMES=YES;	DATAROW=2;
RUN;

PROC IMPORT OUT=HCMC_2020_12
		DATAFILE='/folders/myfolders/DANA-Group-6-team-project/HoChiMinhCity_PM2.5_2020_12_MTD.csv' 
		DBMS=CSV REPLACE; GETNAMES=YES;	DATAROW=2;
RUN;

PROC IMPORT OUT=HCMC_2021_02
		DATAFILE='/folders/myfolders/DANA-Group-6-team-project/HoChiMinhCity_PM2.5_2021_02_MTD.csv' 
		DBMS=CSV REPLACE; GETNAMES=YES;	DATAROW=2;
RUN;

data HCMC_merged;
	set hcmc_2016_12 hcmc_2017_12 hcmc_2018_12 hcmc_2019_12 hcmc_2020_12 hcmc_2021_02;
	by date__lt_;
run;
********;

title 'Error in QC Name (not Valid labeled as Valid) according to values';
proc sql ;
	select  Date__LT_,  AQI,  Nowcast_conc_, Raw_Conc_,  QC_Name from HCMC_merged
	where qc_name like 'Valid' and 
	(raw_conc_ < 0 or raw_conc_ > 500 or nowcast_conc_ <0 or nowcast_conc_ >500 or aqi < 0 or aqi > 500) 
	order by date__lt_;
	
	update HCMC_merged set QC_Name = 'Inval' 
	where qc_name like 'Valid' and 
	(raw_conc_ < 0 or raw_conc_ > 500 or nowcast_conc_ <0 or nowcast_conc_ >500 or aqi < 0 or aqi > 500) ;
run;
title;

title 'Error in QC Name (Valid labeled as Non-Valid) according to values';
proc sql ;
	select  Date__LT_,  AQI,  Nowcast_conc_, Raw_Conc_,  QC_Name from HCMC_merged
	where qc_name not like 'Valid' 
	and  (raw_conc_ > 0 and raw_conc_ < 500 ) 
	and (nowcast_conc_ > 0 and nowcast_conc_ <500) 
	and (aqi > 0 and aqi < 500) 
	order by date__lt_;
	
	update HCMC_merged set QC_Name = 'Valid' 
	where qc_name not like 'Valid' 
	and  (raw_conc_ > 0 and raw_conc_ < 500 ) 
	and (nowcast_conc_ > 0 and nowcast_conc_ <500) 
	and (aqi > 0 and aqi < 500) ;
run;
title;

*update QC_Name according to the values provided;


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

* replace missing Raw_Conc_ ;

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
	
* Calculate NowCats_Conc_;
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

	IF AQI_CATEGORY='' THEN DO;

		IF AQI >=0 AND AQI <=50 THEN AQI_CATEGORY='Good';

		IF AQI >=51 AND AQI <=100 THEN AQI_CATEGORY='Moderate';

		IF AQI >=101 AND AQI <=150 THEN AQI_CATEGORY='Unhealthy for Sensitive Groups';

		IF AQI >=151 AND AQI <=200 THEN AQI_CATEGORY='Unhealthy';

		IF AQI >=201 AND AQI <=300 THEN AQI_CATEGORY='Very Unhealthy';

		IF AQI >=301 AND AQI <=500 THEN AQI_CATEGORY='Hazardous';

		END;		
RUN;
********************************************************************************************************************************;

* SInce all wrong values have been treated, we can change 'Inval' and 'Missi' in QC_Name to 'Valid';

data hcmc_merged;
set hcmc_merged;

if qc_name='Inval' or qc_name ='Missi' then qc_Name = 'Valid';
run;	

*************************************************************************************************************

*Import External Dataset;

*Drop SLP (no valid data) and PP (not considering);

*Convert Day to propoer date format;
	
DATA WEATHER_2016_12;
infile '/folders/myfolders/DANA-Group-6-team-project/HCMC all weather Dec_2016.csv' firstobs=2 missover dsd;
	format Day 2. Temp_Avg 4.1 Temp_Max 4.1 Temp_Min 4.1 SLP 4.1 Humidity_Avg 4.1 PP 4.1 Visibility_Avg  4.1 Wind_Speed_Avg 4.1 Wind_Speed_Max 4.1;
	input  Day Temp_Avg Temp_Max Temp_Min SLP Humidity_Avg PP Visibility_Avg  Wind_Speed_Avg Wind_Speed_Max;
run;

data WEATHER_2016_12; 
	format  date Date9.;
	set WEATHER_2016_12; 
	drop SLP PP d;
	d =  PUT(Day, $10.)||'DEC2016';
	date = input(substr(strip(d),1,10),DATE9.);
run; 

DATA WEATHER_2017_12;
infile '/folders/myfolders/DANA-Group-6-team-project/HCMC all weather Dec_2017.csv' firstobs=2 missover dsd;
	format Day 2. Temp_Avg 4.1 Temp_Max 4.1 Temp_Min 4.1 SLP 4.1 Humidity_Avg 4.1 PP 4.1 Visibility_Avg  4.1 Wind_Speed_Avg 4.1 Wind_Speed_Max 4.1;
	input  Day Temp_Avg Temp_Max Temp_Min SLP Humidity_Avg PP Visibility_Avg  Wind_Speed_Avg Wind_Speed_Max;
run;

data WEATHER_2017_12; 
	format  date Date9.;
	set WEATHER_2017_12; 
	drop SLP PP d;
	d =  PUT(Day, $10.)||'DEC2017';
	date = input(substr(strip(d),1,10),DATE9.);
run; 

DATA WEATHER_2018_12;
infile '/folders/myfolders/DANA-Group-6-team-project/HCMC all weather Dec_2018.csv' firstobs=2 missover dsd;
	format Day 2. Temp_Avg 4.1 Temp_Max 4.1 Temp_Min 4.1 SLP 4.1 Humidity_Avg 4.1 PP 4.1 Visibility_Avg  4.1 Wind_Speed_Avg 4.1 Wind_Speed_Max 4.1;
	input  Day Temp_Avg Temp_Max Temp_Min SLP Humidity_Avg PP Visibility_Avg  Wind_Speed_Avg Wind_Speed_Max;
run;

data WEATHER_2018_12; 
	format  date Date9.;
	set WEATHER_2018_12; 
	drop SLP PP d;
	d =  PUT(Day, $10.)||'DEC2018';
	date = input(substr(strip(d),1,10),DATE9.);
run; 

DATA WEATHER_2019_12;
infile '/folders/myfolders/DANA-Group-6-team-project/HCMC all weather Dec_2019.csv' firstobs=2 missover dsd;
	format Day 2. Temp_Avg 4.1 Temp_Max 4.1 Temp_Min 4.1 SLP 4.1 Humidity_Avg 4.1 PP 4.1 Visibility_Avg  4.1 Wind_Speed_Avg 4.1 Wind_Speed_Max 4.1;
	input  Day Temp_Avg Temp_Max Temp_Min SLP Humidity_Avg PP Visibility_Avg  Wind_Speed_Avg Wind_Speed_Max;
run;

data WEATHER_2019_12; 
	format  date Date9.;
	set WEATHER_2019_12; 
	drop SLP PP d;
	d =  PUT(Day, $10.)||'DEC2019';
	date = input(substr(strip(d),1,10),DATE9.);
run; 

DATA WEATHER_2020_12;
infile '/folders/myfolders/DANA-Group-6-team-project/HCMC all weather Dec_2020.csv' firstobs=2 missover dsd;
	format Day 2. Temp_Avg 4.1 Temp_Max 4.1 Temp_Min 4.1 SLP 4.1 Humidity_Avg 4.1 PP 4.1 Visibility_Avg  4.1 Wind_Speed_Avg 4.1 Wind_Speed_Max 4.1;
	input  Day Temp_Avg Temp_Max Temp_Min SLP Humidity_Avg PP Visibility_Avg  Wind_Speed_Avg Wind_Speed_Max;
run;

data WEATHER_2020_12; 
	format  date Date9.;
	set WEATHER_2020_12; 
	drop SLP PP d;
	d =  PUT(Day, $10.)||'DEC2020';
	date = input(substr(strip(d),1,10),DATE9.);
run;

DATA WEATHER_2021_12;
infile '/folders/myfolders/DANA-Group-6-team-project/HCMC all weather Feb_2021.csv' firstobs=2 missover dsd obs=29;
	format Day 2. Temp_Avg 4.1 Temp_Max 4.1 Temp_Min 4.1 SLP 4.1 Humidity_Avg 4.1 PP 4.1 Visibility_Avg  4.1 Wind_Speed_Avg 4.1 Wind_Speed_Max 4.1;
	input  Day Temp_Avg Temp_Max Temp_Min SLP Humidity_Avg PP Visibility_Avg  Wind_Speed_Avg Wind_Speed_Max;
run;

data WEATHER_2021_12 (sortedby=date); 
	format  date Date9.;
	set WEATHER_2021_12; 
	drop SLP PP d;
	d =  PUT(Day, $10.)||'FEB2021';
	date = input(substr(strip(d),1,10),DATE9.);
run; 

*create merged dataset;

data Weather_merged (drop= Day Temp_Max Temp_Min Wind_Speed_Max);
	set WEATHER_2016_12 WEATHER_2017_12 WEATHER_2018_12 WEATHER_2019_12 WEATHER_2020_12 WEATHER_2021_12;
	by date;
run;
/*  */
/* proc sql; */
/* 	Create table Weather_merged as */
/* 	(select * from Weather_merged */
/* 	 union */
/* 	 select * from WEATHER_2021_12); */
/* run; */

*********************************************************************************************************

*create date column in HCMC_Merged;

data HCMC_Weather_Merged (drop= Site Parameter Date__LT_ Day Hour Month Year Duration  Conc__Unit  QC_Name); 
	format date DATE9.;
	set WORK.hcmc_merged;
	date= datepart(Date__LT_);
run;

*Create  HCMC_Weather_Merged - convert required hourly values to daily values;

proc summary data=HCMC_Weather_Merged nway;
  var  AQI  Nowcast_conc_  Raw_Conc_ ;
  class date;
  output out=HCMC_Weather_Merged (drop=_:)
           mean=Daily_AQI Daily_NowCast_Conc Daily_Raw_Conc;
run;

DATA HCMC_Weather_Merged;
	format AQI_Category $30.;
	*format Daily_AQI 6.2;
	SET HCMC_Weather_Merged;
	year = year(date);
	Daily_AQI = round(Daily_AQI,0.01);
	Daily_NowCast_Conc = round(Daily_NowCast_Conc,0.01);
	Daily_Raw_Conc = round(Daily_Raw_Conc,0.01);
	IF Daily_AQI >=0 AND Daily_AQI <=50 THEN AQI_CATEGORY='Good';
	IF Daily_AQI >=51 AND Daily_AQI <=100 THEN AQI_CATEGORY='Moderate';
	IF Daily_AQI >=101 AND Daily_AQI <=150 THEN AQI_CATEGORY='Unhealthy for Sensitive Groups';
	IF Daily_AQI >=151 AND Daily_AQI <=200 THEN AQI_CATEGORY='Unhealthy';
	IF Daily_AQI >=201 AND Daily_AQI <=300 THEN AQI_CATEGORY='Very Unhealthy';
	IF Daily_AQI >=301 AND Daily_AQI <=500 THEN AQI_CATEGORY='Hazardous';
		
RUN;

*Add daily Weather data into HCMC_Weather_Merged for analysis;

/* proc sql; */
/* 	select * from HCMC_Weather_Merged */
/* 	where year=2021; */
/* 	 */
/* 	Select * from Weather_merged */
/* 	where date > '31-JAN-2021'; */
/* run; */

PROC SQL;
	Create table HCMC_Weather_Merged as
	Select  HCMC_Weather_Merged.date, Year, Daily_AQI, Daily_NowCast_Conc, Daily_Raw_Conc,Temp_Avg, Humidity_Avg , Visibility_Avg, Wind_Speed_Avg
	from HCMC_Weather_Merged inner join Weather_merged
	On HCMC_Weather_Merged.date = Weather_merged.date;
run;

**********************************************************************************************************;



	

		