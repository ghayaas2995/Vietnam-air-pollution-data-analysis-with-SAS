*Importing the 2 datasets into sas*;

/*1st file:*/
PROC IMPORT OUT=HCMC_2016_12
		DATAFILE='/folders/myfolders/Team project/HoChiMinhCity_PM2.5_2016_12_MTD.csv' 
		DBMS=CSV REPLACE;
	GETNAMES=YES;
	DATAROW=2;
RUN;

/*2nd file:*/
PROC IMPORT OUT=HCMC_2017_12
		DATAFILE='/folders/myfolders/Team project/HoChiMinhCity_PM2.5_2017_12_MTD.csv' 
		DBMS=CSV REPLACE;
	GETNAMES=YES;
	DATAROW=2;
RUN;

/*3rd file:*/
PROC IMPORT OUT=HCMC_2018_12
		DATAFILE='/folders/myfolders/Team project/HoChiMinhCity_PM2.5_2018_12_MTD.csv' 
		DBMS=CSV REPLACE;
	GETNAMES=YES;
	DATAROW=2;
RUN;

/*4th file:*/
PROC IMPORT OUT=HCMC_2019_12
		DATAFILE='/folders/myfolders/Team project/HoChiMinhCity_PM2.5_2019_12_MTD.csv' 
		DBMS=CSV REPLACE;
	GETNAMES=YES;
	DATAROW=2;
RUN;

/*5th file:*/
PROC IMPORT OUT=HCMC_2021_02
		DATAFILE='/folders/myfolders/Team project/HoChiMinhCity_PM2.5_2021_02_MTD.csv' 
		DBMS=CSV REPLACE;
	GETNAMES=YES;
	DATAROW=2;
RUN;

/*6th file:*/
PROC IMPORT OUT=HCMC_2020_12
		DATAFILE='/folders/myfolders/Team project/HoChiMinhCity_PM2.5_2020_12_MTD.csv' 
		DBMS=CSV REPLACE;
	GETNAMES=YES;
	DATAROW=2;
RUN;









































































