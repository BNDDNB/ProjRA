import csv, sqlite3
import pandas as pd
import numpy as np
from sqlite3 import Error

'''
create a database connection to a database that resides
    in the memory
for the purpose of creating database in memory based on the projects
during the input, dimentions of data include and their corresponded columns
	GEO_CODE (POR)	GEO_CODE
	GEO_LEVEL	GEO_LVL
	GEO_NAME	GEO_NAME
	DIM: Registered or Treaty Indian status (3)	REG_STAT
	Member ID: Registered or Treaty Indian status (3)	REG_STAT_IND
	DIM: Age (9)	AGE_GRP
to-do: parameterize the column list during the ingestion
'''

def query_exec(con, cmd_lst):
    cur = con.cursor() 
    for each in cmd_lst:
        cur.execute(each)


def data_reader(con, filename):
	#creating table and prepare for the filenames
	cur = con.cursor()
	cur.execute(tbl_create) 
	to_db = []
	with open(filename,'rt') as fin: 
	    reader = csv.reader(fin, delimiter = ',')
	    next(reader, None)
	    for row in reader:
	        content = tuple(row[i] for i in included_cols)
	        to_db.append(content)
	    
	# this inserts the records into the sqlite db in memory
	cur.executemany("INSERT INTO CensusT (GEO_CODE, GEO_LVL, GEO_NAME, REG_STAT, REG_STAT_IND, AGE_GRP, AGE_GRP_IND, \
	SEX, SEX_IND, INC_STAT, INC_STAT_IND, TTL_STAT,  AB_ID, SING_AB, FNATION, METIS, \
	INUK, MUL_AB, OS_AB, NOT_AB) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);", to_db)

	#update tasks
	cmd_lst = [tbl_altr,tbl_altr2,tbl_update]
	query_exec(con, cmd_lst)
	con.commit()
	
'''
	this function created to finish the initial requirements based on dataset 1
	requirement 1a uses the following which is the only one that requires proportion
	"SELECT SUM(AB_ID) / SUM(TTL_STAT) as AB_VAL, SUM(NOT_AB)/SUM(TTL_STAT) as NON_AB_VAL FROM CensusT "
'''
def proc_a (con):
	region_list = list(region_d.keys())
	df = pd.DataFrame(columns = ['Aboraginal Identity', 'None Aboraginal Identity'], \
						index = region_list)
	clause = "SELECT GEO_REG_NAME, ROUND(SUM(AB_ID) / SUM(TTL_STAT),2) as AB_VAL, ROUND(SUM(NOT_AB)/SUM(TTL_STAT),2) as NON_AB_VAL \
				FROM CensusT \
				WHERE REG_STAT_IND = 1 AND AGE_GRP_IND = 1 AND SEX_IND = 1 AND INC_STAT_IND = 1 AND GEO_REG_IND >= 1\
				GROUP BY GEO_REG_NAME;"
	cur = con.cursor()
	cur.execute(clause)
	rows = cur.fetchall()
	for i in range(0, len(rows)):
		df.at[rows[i][0],'Aboraginal Identity'] = rows[i][1]
		df.at[rows[i][0],'None Aboraginal Identity'] = rows[i][2]
	df.to_csv('./PopulationProportion.csv')

def proc_b(con):
	region_list = list(region_d.keys())
	df = pd.DataFrame(columns = ['Aboraginal Identity', 'None Aboraginal Identity'], \
						index = region_list)
	clause = "SELECT GEO_REG_NAME, AVG(AB_ID) AS AVGAB, AVG(NOT_AB) AS AVGNAB FROM CensusT \
				WHERE REG_STAT_IND = 1 AND AGE_GRP_IND = 1 AND SEX_IND = 1 AND INC_STAT_IND = 5 AND GEO_REG_IND >= 1\
				GROUP BY GEO_REG_NAME;"
	cur = con.cursor()
	cur.execute(clause)
	rows = cur.fetchall()
	for i in range(0, len(rows)):
		df.at[rows[i][0],'Aboraginal Identity'] = rows[i][1]
		df.at[rows[i][0],'None Aboraginal Identity'] = rows[i][2]
	df.to_csv('./AvgTtlIncome.csv')

def proc_c(con):
	region_list = list(region_d.keys())
	df = pd.DataFrame(columns = ['Aboriginal Male', 'Aboriginal Female', 'Non-Aboriginal Male', 'Non-Aboriginal Female'], \
						index = region_list)
	sel_ppt = "SELECT GEO_REG_NAME, SUM(AB_ID) AS AB_VAL, SUM(NOT_AB) AS NON_AB_VAL FROM CensusT \
				WHERE REG_STAT_IND = 1 AND AGE_GRP_IND = 1 AND SEX_IND = 1 AND INC_STAT_IND = 1 AND GEO_REG_IND >= 1\
				GROUP BY GEO_REG_NAME;"
	sel_ppm = "SELECT GEO_REG_NAME, SUM(AB_ID) AS AB_VAL, SUM(NOT_AB) AS NON_AB_VAL FROM CensusT \
				WHERE REG_STAT_IND = 1 AND AGE_GRP_IND = 1 AND SEX_IND = 2 AND INC_STAT_IND = 1 AND GEO_REG_IND >= 1\
				GROUP BY GEO_REG_NAME;"
	sel_ppf = "SELECT GEO_REG_NAME, SUM(AB_ID) AS AB_VAL, SUM(NOT_AB) AS NON_AB_VAL FROM CensusT \
				WHERE REG_STAT_IND = 1 AND AGE_GRP_IND = 1 AND SEX_IND = 3 AND INC_STAT_IND = 1 AND GEO_REG_IND >= 1\
				GROUP BY GEO_REG_NAME;"
	cur = con.cursor()
	cur.execute(sel_ppm)
	rowsm = cur.fetchall()
	cur.execute(sel_ppf)
	rowsf = cur.fetchall()
	cur.execute(sel_ppt)
	rowst = cur.fetchall()

	for i in range(0, len(rowst)):
		df.at[rowst[i][0],'Aboriginal Male'] = float("{0:.2f}".format(rowsm[i][1]/rowst[i][1]))
		df.at[rowst[i][0],'Aboriginal Female'] = float("{0:.2f}".format(rowsf[i][1]/rowst[i][1]))
		df.at[rowst[i][0],'Non-Aboriginal Male'] = float("{0:.2f}".format(rowsm[i][2]/rowst[i][2]))
		df.at[rowst[i][0],'Non-Aboriginal Female'] = float("{0:.2f}".format(rowsf[i][2]/rowst[i][2]))

	df.to_csv('./MaleFemaleProportion.csv')


def proc_d(con):
	region_list = list(region_d.keys())
	df = pd.DataFrame(columns = ['Max Aboraginal Identity Age Group'], \
						index = region_list)
	sel_max = "SELECT GEO_REG_NAME,AGE_GRP, MAX(AB_ID) FROM CensusT \
				WHERE REG_STAT_IND = 1 AND AGE_GRP_IND > 1 AND SEX_IND = 1 AND INC_STAT_IND = 1 AND GEO_REG_IND >= 1\
				GROUP BY GEO_REG_NAME;"

	cur = con.cursor()
	cur.execute(sel_max)
	rows = cur.fetchall()
	for i in range(0,len(rows)):
		df.at[rows[i][0],'Max Aboraginal Identity Age Group'] = rows[i][1]
	df.to_csv('./MaxAgeGroup.csv')


def init():
	global region_d, tbl_create, tbl_altr, tbl_altr2, tbl_update, included_cols, datafile
	region_d = {'Western Canada':1, 'Central Canada':2, 'Atlantic Canada':3, 'Northern Canada':4}
	datafile = 'DS1.csv' 
	included_cols = [1,2,3,7,8,10,11,13,14,16,17,19,20,21,22,23,24,25,26,27]
	tbl_altr = "ALTER TABLE CensusT ADD COLUMN GEO_REG_NAME VARCHAR(255); "
	tbl_altr2 = "ALTER TABLE CensusT ADD COLUMN GEO_REG_IND INT;"
	tbl_create = "CREATE TABLE CensusT (GEO_CODE INT, GEO_LVL INT, GEO_NAME VARCHAR(255), REG_STAT VARCHAR(255), REG_STAT_IND INT, AGE_GRP VARCHAR(255), \
			 		AGE_GRP_IND INT, SEX VARCHAR(255), SEX_IND INT, INC_STAT VARCHAR(255), INC_STAT_IND INT, \
			 		TTL_STAT DOUBLE,  AB_ID DOUBLE, SING_AB DOUBLE, FNATION DOUBLE, METIS DOUBLE, INUK DOUBLE, MUL_AB DOUBLE, OS_AB DOUBLE, NOT_AB DOUBLE);"
	tbl_update = "UPDATE CensusT SET GEO_REG_NAME = (CASE \
	            WHEN GEO_NAME IN ('Ontario','Quebec') THEN 'Central Canada'\
	     		WHEN GEO_NAME IN ('British Columbia','Alberta','Saskatchewan','Manitoba') THEN 'Western Canada' \
	     		WHEN GEO_NAME IN ('New Brunswick','Prince Edward Island','Nova Scotia','Newfoundland and Labrador') THEN 'Atlantic Canada' \
	     		WHEN GEO_NAME IN ('Yukon','Northwest Territories','Nunavut') THEN 'Northern Canada' \
	    		ELSE 'OTHERLEVEL' \
	    		END),\
	    		GEO_REG_IND = (CASE \
	            WHEN GEO_NAME IN ('Ontario','Quebec') THEN 2\
	     		WHEN GEO_NAME IN ('British Columbia','Alberta','Saskatchewan','Manitoba') THEN 1 \
	     		WHEN GEO_NAME IN ('New Brunswick','Prince Edward Island','Nova Scotia','Newfoundland and Labrador') THEN 3 \
	     		WHEN GEO_NAME IN ('Yukon','Northwest Territories','Nunavut') THEN 4 \
	    		ELSE 0 END);"


def main():
	con = sqlite3.connect(":memory:")
	init()
	data_reader(con, datafile)
	proc_a(con)
	proc_b(con)
	proc_c(con)
	proc_d(con)


if __name__ == '__main__':
    main()



'''
the following function can be used to create the database on disk
'''
 
# def create_connection(db_file):
#     """ create a database connection to a SQLite database """
#     try:
#         conn = sqlite3.connect(db_file)
#         print(sqlite3.version)
#     except Error as e:
#         print(e)
#     finally:
#         conn.close()