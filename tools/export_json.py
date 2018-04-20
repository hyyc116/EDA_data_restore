#coding:utf-8
'''
从数据库中将indeed的数据以及youreconomy的数据导出为json
'''
from db_util import *


## 加载位置信息
def load_location():
	logging.info('load location ...')

	query_op = dbop()
	## 加载state的数据
	sql = 'select id,abbreviation from state'
	sid_abbr = {}
	for sid,abbreviation in query_op.query_database(sql):
		sid_abbr[sid] = abbreviation

	## 加载 county的数据
	sql = 'select id,name,sid from county'
	cid_sid = {}
	cid_name = {}
	for cid,name,sid in query_op.query_database(sql):
		cid_sid[cid] = sid

		cid_name[cid] = name
	

	## 加载city的数据

	sql= 'select id,name,sid,ctid from city'
	city_top = {}
	cityid_name = {}

	for cityid,name,sid,cid in query_op.query_database(sql):
		city_top[cityid] = [sid,cid]
		cityid_name[cityid] = name

	return sid_abbr,cid_sid,cid_name,city_top,cityid_name

	query_op.close_db()

def export_ye(location):
	logging.info('export ye ...')
	sid_abbr,cid_sid,cid_name,city_top,cityid_name = location
	## 将your economy的数据进行导出
	sql = 'select cid,year,sid,toptype,subtype,value from attribute'
	query_op = dbop()
	sid_cid_year_toptype_subtype_value = defaultdict(lambda : defaultdict(lambda : defaultdict(lambda : defaultdict(dict))))
	for cid,year,sid,toptype,subtype,value in query_op.query_database(sql):
		
		if value=='NA':
			value = 0.0
		else:
			if '$' in value:
				value = value.replace('$','').replace(',','')
				if value.endswith('B'):
					value = float(value[:-1])*1000*1000*1000
				elif value.endswith('M'):
					value = float(value[:-1])*1000*1000
				elif value.endswith('K'):
					value = float(value[:-1])*1000
				elif value.endswith('T'):
					value = float(value[:-1])*1000*1000*1000*1000
				else:
					value = float(value)
			else:
				value = float(value.replace('*','').replace(',',''))

		sid_cid_year_toptype_subtype_value[sid][cid][year][toptype][subtype] = value

	logging.info('start to export ...')

	all_data = []
	for sid in sid_cid_year_toptype_subtype_value.keys():
		abbr = sid_abbr[sid]
		logging.info('export state {:} ..'.format(abbr))
		for cid in sid_cid_year_toptype_subtype_value[sid].keys():
			county_name = cid_name[cid]
			for year in sid_cid_year_toptype_subtype_value[sid][cid].keys():
				obj = {}
				obj['state']  = abbr
				obj['county'] = county_name
				toptype_subtype_value = sid_cid_year_toptype_subtype_value[sid][cid][year]
				## 对于每一年，我们需要的数据
				## business by type, 中三种type
				obj['RESIDENT'] = toptype_subtype_value['allbusinesses']['RESIDENT']
				obj['NONCOMMERCIAL'] = toptype_subtype_value['allbusinesses']['NONCOMMERCIAL']
				obj['NONRESIDENT'] = toptype_subtype_value['allbusinesses']['NONRESIDENT']

				## job中的几个阶段
				obj['SELF EMPLOYEE'] = toptype_subtype_value['alljobs']['Self-Employed (1)']
				obj['2-9 EMPLOYEES'] = toptype_subtype_value['alljobs']['2-9 Employees']
				obj['10-99 EMPLOYEES'] = toptype_subtype_value['alljobs']['10-99 Employees']
				obj['100-499 EMPLOYEES'] = toptype_subtype_value['alljobs']['100-499 Employees']
				obj['500+ EMPLOYEES'] = toptype_subtype_value['alljobs']['500+ Employees']

				## GAIN LOST
				obj['GAINED'] = toptype_subtype_value['totalgained']['GAINED']
				obj['LOST'] = toptype_subtype_value['totallost']['LOST']

				##
				obj['ALL SALES'] = toptype_subtype_value['allsales']['GAINED']
				obj['SALES PER EMPLOYEE'] = toptype_subtype_value['salesperemployee']['SALES PER EMPLOYEE']
				obj['SALES PER BUSINESS'] = toptype_subtype_value['salesperestablishment']['SALES PER BUSINESS']

				all_data.append(obj)

	open('data/yedata.json','w').write(json.dumps(all_data))

	logging.info('Done')

def export_indeed():
	pass




if __name__ == '__main__':

	label = sys.argv[1]

	if label=='export_ye':
		location = load_location()
		export_ye(location)

	else:
		logging.info('No such label.')








