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

def county_topo_id():
	## 首先读state对应的ID
	sid_abbr = {}
	for line in open('us-state.txt'):
		line = line.strip()
		_id,abbr,name = line.split('\t')
		sid_abbr[_id]=abbr

	abbr_name_topoid = defaultdict(dict)
	for line in open('us-counties.txt'):
		line = line.strip()
		_id,name = line.split('\t')
		sid = _id[:-3]
		abbr = sid_abbr[sid]
		name = pro_county_name(name,abbr)
		abbr_name_topoid[abbr][name]=_id

	return abbr_name_topoid


def export_ye(location,abbr_name_topoid):
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
    lines = ['state,county,year,topoid,RESIDENT,NONCOMMERCIAL,NONRESIDENT,SELF EMPLOYEE,2-9 EMPLOYEES,10-99 EMPLOYEES,100-499 EMPLOYEES,500+ EMPLOYEES,GAINED,LOST,ALL SALES,SALES PER EMPLOYEE,SALES PER BUSINESS']
    for sid in sid_cid_year_toptype_subtype_value.keys():
        abbr = sid_abbr[sid]
        logging.info('export state {:} ..'.format(abbr))
        for cid in sid_cid_year_toptype_subtype_value[sid].keys():
            county_name = cid_name[cid]
            for year in sid_cid_year_toptype_subtype_value[sid][cid].keys():
                obj = {}
                obj['state']  = abbr
                county = pro_county_name(county_name,abbr)
                obj['county'] = county
                obj['year'] = year
                obj['topoid'] = abbr_name_topoid[abbr][county]
                toptype_subtype_value = sid_cid_year_toptype_subtype_value[sid][cid][year]
                ## 对于每一年，我们需要的数据
                ## business by type, 中三种type
                resident = toptype_subtype_value['allbusinesses']['RESIDENT']
                nonresident = toptype_subtype_value['allbusinesses']['NONRESIDENT']
                noncommercial = toptype_subtype_value['allbusinesses']['NONCOMMERCIAL']
                obj['RESIDENT'] = resident
                obj['NONCOMMERCIAL'] = noncommercial
                obj['NONRESIDENT'] = nonresident

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
                obj['ALL SALES'] = toptype_subtype_value['allsales']['ALL']
                obj['SALES PER EMPLOYEE'] = toptype_subtype_value['salesperemployee']['SALES PER EMPLOYEE']
                obj['SALES PER BUSINESS'] = toptype_subtype_value['salesperestablishment']['SALES PER BUSINESS']

                all_data.append(obj)

    open('data/yedata.json','w').write(json.dumps(all_data))

    labels = lines[0].split(',')

    for obj in all_data:
    	line = []

    	for label in labels:
    		line.append(str(obj[label]))

    	lines.append(','.join(line))

    open('data/yedata.csv','w').write('\n'.join(lines))

    query_op.close_db()
    logging.info('Done')


def pro_county_name(name,state):

    name = name.lower().replace(' city','').replace('sainte.','saint').replace('st.','saint').replace('st','saint').replace('-',' ').replace('\'','').strip()
    if name.startswith('de') or name.startswith('la'):
        name = name.replace(' ','')
    name = name.capitalize()
    return  name+"("+state+")"   


def export_indeed(location,abbr_name_topoid):
    logging.info('export indeed ...')
    sid_abbr,cid_sid,cid_name,city_top,cityid_name = location

    query_op = dbop()
    ## 将company的数据与id进行对应
    company_to_id = {}
    company_set = set([])
    _id_to_company = {}
    sql= 'select cityid,company,position,postype,publishdate,salary from job'
    all_data= []
    for cityid,company,position,postype,publishdate,salary in query_op.query_database(sql):
        sid,cid = city_top[cityid]

        abbr = sid_abbr[sid]
        county_name = cid_name[cid]
        # print county_name
        county_name = pro_county_name(county_name,abbr)
        # print county_name

        if abbr_name_topoid[abbr].get(county_name,-1)==-1:
        	continue
        obj={}
        obj['state'] = abbr
        obj['county'] = county_name
        obj['topoid'] = abbr_name_topoid[abbr][county_name]
        company = company.decode('utf-8',errors='ignore')
        
        ##给company进行编号
        if company not in company_set:
        	company_to_id[company] = len(company_set)
        	company_set.add(company)
        	_id_to_company[len(company_set)] = company


        obj['company'] = company
        obj['position'] = position
        obj['jobtype'] = postype
        obj['salary'] = salary
        obj['publishdate'] = publishdate

        all_data.append(obj)

    query_op.close_db()

    open('data/indeed.json','w').write(json.dumps(all_data))

    lines =['state,company,topoid,jobtype,salary,publishdate']

    labels = lines[0].split(',')

    for obj in all_data:
    	line = []

    	for label in labels:
    		if label == 'company':
    			line.append(str(company_to_id[obj[label]]))
    		else:
    			line.append(str(obj[label]))

    	lines.append(','.join(line))

    open('data/indeed.csv','w').write('\n'.join(lines))

    open('data/company_id_to_name.json','w').write(json.dumps(_id_to_company))

    logging.info('Done')



if __name__ == '__main__':

    label = sys.argv[1]

    if label=='export_ye':
        location = load_location()
        abbr_name_topoid = county_topo_id()
        export_ye(location,abbr_name_topoid)
    elif label=='export_indeed':
        location = load_location()
        abbr_name_topoid = county_topo_id()
        export_indeed(location,abbr_name_topoid)
    else:
        logging.info('No such label.')








