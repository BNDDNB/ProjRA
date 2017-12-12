import csv, sqlite3
import pandas as pd
import numpy as np
from sqlite3 import Error
import matplotlib.pyplot as plt
from io import BytesIO
import base64
import os
import folium
from folium.plugins import HeatMap
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

def createHMData(con):
	cur = con.cursor()
	clause = "SELECT GEO_NAME ,AB_ID FROM CensusT WHERE GEO_LVL >= 2 AND REG_STAT_IND = 1 AND AGE_GRP_IND = 1 \
				AND SEX_IND = 1 AND INC_STAT_IND = 1;"
	df = pd.read_csv('CityLoc.csv',index_col=0, header = 0)
	df['Value'] = np.nan
	cur.execute(clause)
	rows = cur.fetchall()
	for i in range(0,len(rows)):
		try:
			df.at[rows[i][0],'Value'] = rows[i][1]
		except:
			pass
	df = df[pd.notnull(df['Value'])]
	df = df[pd.notnull(df['Lat'])]
	df.to_csv('citylocandvalue.csv')


def make_heatmap():
	df = pd.read_csv('citylocandvalue.csv')
	max_amount = float(df['Value'].max())
	m = folium.Map([56,-95], tiles='Stamen Terrain', zoom_start=4)
	hm_wide = HeatMap(list(zip(df.Lat.values, df.Lon.values, df.Value.values)), 
               min_opacity=0.2,
               max_val=max_amount,
               radius=17, blur=15, 
               max_zoom=1)
	m.add_child(hm_wide)
	m.save(os.path.join('static/results', 'Heatmap.html'))

def query_exec(con, cmd_lst):
    cur = con.cursor() 
    for each in cmd_lst:
        cur.execute(each)

def select_mkr(identity_para = '', avg_para = False):
	sel = "SELECT "
	if identity_para != '' and avg_para:
		sel = sel + "AVG(" + select_d[identity_para] + ") FROM CensusT "
	elif identity_para != '':
		sel = sel + "SUM(" + select_d[identity_para] + ") FROM CensusT "
	elif avg_para:
		sel = sel + "AVG(AB_ID), AVG(NOT_AB) FROM CensusT "
	else:
		sel = sel + "SUM(AB_ID), SUM(NOT_AB) FROM CensusT "
	return sel


def query_cityList(con):
	cur = con.cursor()
	cur.execute (sel_cityList)
	rows = cur.fetchall()
	rows = list (map(lambda x:x[0], rows))
	rows = [' '] + rows
	return rows

def query_mkr(geo_para = '', age_para = '',  sex_para = '', inc_para = '', city_para = ' '):
	#the first condition is here because its necessary but doesnt change
	result = " AND REG_STAT_IND = " + str(1)
	if (geo_para != '' and city_para == ' '):
		result += " AND GEO_REG = " + str(region_d[geo_para])
	if (city_para != ' '):
		result += " AND GEO_NAME = \'" + city_para + "\'"
	if(age_para != ''):
		result += " AND AGE_GRP_IND = " + str(age_d[age_para])
	if(sex_para != ''):
		result += " AND SEX_IND = " + str(sex_d[sex_para])
	if(inc_para != ''):
		result += " AND INC_STAT_IND = " + str(inc_d[inc_para])
	if(result != ''):
		result = "WHERE " + result[5:]
	return result


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
	cmd_lst = [tbl_altr,tbl_update]
	query_exec(con, cmd_lst)
	con.commit()
	
'''
	this function created to finish the initial requirements based on dataset 1
	requirement 1a uses the following which is the only one that requires proportion
	"SELECT SUM(AB_ID) / SUM(TTL_STAT) as AB_VAL, SUM(NOT_AB)/SUM(TTL_STAT) as NON_AB_VAL FROM CensusT "
'''
def proc_tabular (con, iden_para, df, age_para, sex_para, inc_para, geo_para = '', city_para =  ' '):
	if (inc_para == 'Average total income ($)'):
		sel_clause = select_mkr(iden_para, True)
	else:
		sel_clause = select_mkr(iden_para,False)

	where_clause = query_mkr(geo_para, age_para, sex_para, inc_para , city_para)
	cur = con.cursor()
	cur.execute(sel_clause + where_clause)
	rows = cur.fetchall()[0]
	df.iloc[0] = rows


def plotter(con, age, sex, region, city, identity):
	if (identity == ''):
		titles = 'Aboraginal Identity vs None Aboraginal Identity'
		df1 = pd.DataFrame(columns = ['Aboraginal Identity', 'None Aboraginal Identity'], \
		                index = ['Population'])
		df2 = pd.DataFrame(columns = ['Aboraginal Identity', 'None Aboraginal Identity'], \
		                index = ['Average Total Income'])
	else:
		titles = identity
		df1 = pd.DataFrame(columns = [identity], \
		                index = ['Population'])
		df2 = pd.DataFrame(columns = [identity], \
		                index = ['Average Total Income'])
	proc_tabular(con, identity, df1, age, sex, 'Total - Income statistics', region, city)
	proc_tabular(con, identity, df2, age, sex, 'Average total income ($)', region, city)
	fig, axes = plt.subplots(nrows=1, ncols=2,figsize = (12,12))
	ax = df1.plot(kind='bar',ax=axes[0],rot=0)
	ax.legend(prop={'size':6})
	ax.set(ylabel = 'Population')
	ax = df2.plot(kind='bar',ax=axes[1],rot=0)
	ax.legend(prop={'size':6})
	ax.set(ylabel = 'Average Income')
	plt.suptitle(titles)
	plt.savefig('./static/pvaplot.png', format='png')
	figfile = BytesIO()
	plt.savefig(figfile, format='png')
	figfile.seek(0)
	figdata_png = base64.b64encode(figfile.getvalue()).decode('ascii')
	return figdata_png


def init():
	global sex_d, region_d, age_d, reg_d, inc_d, select_d, sel_pp, sel_cityList, sel_avginc, tbl_create, tbl_altr, tbl_update, included_cols, datafile
	sex_d = {'Total - Sex': 1, 'Male': 2, 'Female' : 3}
	region_d = {'Western Canada':1, 'Central Canada':2, 'Atlantic Canada':3, 'Northern Canada':4}
	age_d = {'Total - Age':1, '15 to 24 years':2, '25 to 64 years':3,'25 to 54 years':4, \
			'25 to 34 years':5, '35 to 44 years':6, '45 to 54 years':7, '55 to 64 years':8, '65 years and over':9}
	reg_d = {'Total - Population by Registered or Treaty Indian status':1,'Registered or Treaty Indian':2,'Not a Registered or Treaty Indian':3}
	inc_d = {'Total - Income statistics':1, 'Average total income ($)':5}
	select_d = {'Aboriginal':'AB_ID','Non-Aboriginal':'NOT_AB'}
	sel_pp = "SELECT SUM(AB_ID) / SUM(TTL_STAT) as AB_VAL, SUM(NOT_AB)/SUM(TTL_STAT) as NON_AB_VAL FROM CensusT "
	sel_avginc = "SELECT AB_ID, NOT_AB FROM CensusT "
	sel_cityList = "SELECT DISTINCT GEO_NAME FROM CensusT;"
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


