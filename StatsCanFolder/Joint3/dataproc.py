import csv, sqlite3
import pandas as pd
import numpy as np
from sqlite3 import Error



'''
the entire function design is very similar to the base function,
the difference is that the insertion and creation now are 2 parts
that covers for different tables.

Those are created in the 
'''

def query_exec(con, cmd_lst):
    cur = con.cursor() 
    for each in cmd_lst:
        cur.execute(each)


def data_reader(con, filename, tbl_clause, incl_cols, insert_clause):
	#creating table and prepare for the filenames
	cur = con.cursor()
	cur.execute(tbl_clause) 
	to_db = []
	with open(filename,'rt') as fin: 
	    reader = csv.reader(fin, delimiter = ',')
	    next(reader, None)
	    for row in reader:
	        content = tuple(row[i] for i in incl_cols)
	        to_db.append(content)
	    
	# this inserts the records into the sqlite db in memory
	cur.executemany(insert_clause, to_db)
	#update tasks
	con.commit()
	
def proc_tabular (con):
    	
	cur = con.cursor()
	cur.execute ("SELECT DISTINCT GEO_NAME FROM CensusT WHERE GEO_LVL = 1;")
	rows = cur.fetchall()
	provinces = list(map(lambda x:x[0], rows))

	df = pd.DataFrame(columns = ['Aboraginal Identity', 'None Aboraginal Identity', 'High Income'], \
						index = provinces)

	clause1 = "SELECT GEO_NAME, AB_ID, NOT_AB FROM CensusT WHERE GEO_LVL =1 AND REG_STAT_IND = 1 AND AGE_GRP_IND = 1 \
				AND SEX_IND = 1 AND INC_STAT_IND = 1;"
	clause2 = "SELECT GEO_NAME, SUM(INC2015) FROM CensusT2 WHERE GEO_LVL =1 AND AGE_GRP_IND = 1 AND HOUSE_IND = 1\
				AND SEX_IND = 1 AND ERNR_IND = 3 AND INC_STAT_IND >= 14 GROUP BY GEO_NAME;"
	
	cur.execute(clause1)
	rows1 = cur.fetchall()
	cur.execute(clause2)
	rows2 = cur.fetchall()

	for i in range(0,len(rows1)):
		df.at[rows1[i][0],'Aboraginal Identity'] = rows1[i][1]
		df.at[rows1[i][0],'None Aboraginal Identity'] = rows1[i][2]
		df.at[rows2[i][0],'High Income'] = rows2[i][1]
	df.to_csv('./Populations.csv')

'''
global variable initialization
'''

def init():
	global  tbl_create, tbl_create2, insert1, insert2, tbl_altr, tbl_update, \
			included_cols, included_cols2, datafile, datafile2
	datafile = 'DS1.csv' 
	datafile2 = 'DS2.csv'
	included_cols = [1,2,3,7,8,10,11,13,14,16,17,19,20,21,22,23,24,25,26,27]
	included_cols2 = [1,2,3,7,8,10,11,13,14,16,17,19,20,22]
	tbl_altr = "ALTER TABLE CensusT ADD GEO_REG INT;"
	tbl_create = "CREATE TABLE CensusT (GEO_CODE INT, GEO_LVL INT, GEO_NAME VARCHAR(255), REG_STAT VARCHAR(255), REG_STAT_IND INT, AGE_GRP VARCHAR(255), \
			 		AGE_GRP_IND INT, SEX VARCHAR(255), SEX_IND INT, INC_STAT VARCHAR(255), INC_STAT_IND INT, \
			 		TTL_STAT DOUBLE,  AB_ID DOUBLE, SING_AB DOUBLE, FNATION DOUBLE, METIS DOUBLE, INUK DOUBLE, MUL_AB DOUBLE, OS_AB DOUBLE, NOT_AB DOUBLE);"
	tbl_create2 = "CREATE TABLE CensusT2 (GEO_CODE INT, GEO_LVL INT, GEO_NAME VARCHAR(255), HOUSE VARCHAR(255), HOUSE_IND INT, AGE_GRP VARCHAR(255), \
			 		AGE_GRP_IND INT, SEX VARCHAR(255), SEX_IND INT, ERNR VARCHAR(255), ERNR_IND INT, INC_STAT VARCHAR(255), INC_STAT_IND INT, INC2015 INT);"
	insert1 = "INSERT INTO CensusT (GEO_CODE, GEO_LVL, GEO_NAME, REG_STAT, REG_STAT_IND, AGE_GRP, AGE_GRP_IND, \
				SEX, SEX_IND, INC_STAT, INC_STAT_IND, TTL_STAT,  AB_ID, SING_AB, FNATION, METIS, \
				INUK, MUL_AB, OS_AB, NOT_AB) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"
	insert2 = "INSERT INTO CensusT2 (GEO_CODE, GEO_LVL, GEO_NAME, HOUSE,  HOUSE_IND, AGE_GRP, AGE_GRP_IND, \
				SEX, SEX_IND, ERNR, ERNR_IND, INC_STAT, INC_STAT_IND, INC2015) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?);"
	tbl_update = "UPDATE CensusT SET GEO_REG = (CASE \
	            WHEN GEO_NAME IN ('Ontario','Quebec') THEN 2\
	     		WHEN GEO_NAME IN ('British Columbia','Alberta','Saskatchewan','Manitoba') THEN 1 \
	     		WHEN GEO_NAME IN ('New Brunswick','Prince Edward Island','Nova Scotia','Newfoundland and Labrador') THEN 3 \
	     		WHEN GEO_NAME IN ('Yukon','Northwest Territories','Nunavut') THEN 4 \
	    		ELSE 0 END);"

'''
the controller of the entire program
'''
def main():
	try:
		con = sqlite3.connect(":memory:")
		init()
		data_reader(con, datafile, tbl_create,included_cols,insert1)
		cmd_lst = [tbl_altr,tbl_update]
		query_exec(con, cmd_lst)
		data_reader(con, datafile2, tbl_create2,included_cols2,insert2)
		con.commit()
		proc_tabular(con)
	except Error as e:
		print(e)
	finally:
		con.close()

if __name__ == '__main__':
    main()


