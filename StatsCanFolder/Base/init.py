'''
the function initialize needed values values for processing.
primarily used for those that are global constants and configurations
'''

def init():
	global sex_d, region_d, age_d, reg_d, inc_d,tbl_create, tbl_altr, tbl_update, included_cols, datafile
	sex_d = {'Total - Sex': 1, 'Male': 2, 'Female' : 3}
	region_d = {'Western Canada':1, 'Central Canada':2, 'Atlantic Canada':3, 'Northern Canada':4}
	age_d = {'Total - Age':1, '15 to 24 years':2, '25 to 64 years':3,'25 to 54 years':4, \
			'25 to 34 years':5, '35 to 44 years':6, '45 to 54 years':7, '55 to 64 years':8, '65 years and over':9}
	reg_d = {'Total - Population by Registered or Treaty Indian status':1,'Registered or Treaty Indian':2,'Not a Registered or Treaty Indian':3}
	inc_d = {'Total - Income statistics':1, 'Average total income ($)':5}
	datafile = 'DS1.csv' 
	included_cols = [1,2,3,7,8,10,11,13,14,16,17,19,20,21,22,23,24,25,26,27]
	tbl_altr = "ALTER TABLE CensusT ADD GEO_REG INT;"
	tbl_create = "CREATE TABLE CensusT (GEO_CODE INT, GEO_LVL INT, GEO_NAME VARCHAR(255), REG_STAT VARCHAR(255), REG_STAT_IND INT, AGE_GRP VARCHAR(255), \
			 		AGE_GRP_IND INT, SEX VARCHAR(255), SEX_IND INT, INC_STAT VARCHAR(255), INC_STAT_IND INT, \
			 		TTL_STAT DOUBLE,  AB_ID DOUBLE, SING_AB DOUBLE, FNATION DOUBLE, METIS DOUBLE, INUK DOUBLE, MUL_AB DOUBLE, OS_AB DOUBLE, NOT_AB DOUBLE);"
	tbl_update = "UPDATE CensusT SET GEO_REG = (CASE \
	            WHEN GEO_NAME IN ('Ontario','Quebec') THEN 2\
	     		WHEN GEO_NAME IN ('British Columbia','Alberta','Saskatchewan','Manitoba') THEN 1 \
	     		WHEN GEO_NAME IN ('New Brunswick','Prince Edward Island','Nova Scotia','Newfoundland and Labrador') THEN 3 \
	     		WHEN GEO_NAME IN ('Yukon','Northwest Territories','Nunavut') THEN 4 \
	    		ELSE 0 \
	    		END);"