#coding:utf-8
from db_util import *

def store_state():
    ## store state data into database
    rows = []
    abbr_state = {}
    for line in open('data/state.txt'):
        line = line.strip()
        name,abbr = line.split(",")
        name = name.strip()
        rows.append([name,abbr])
        abbr_state[abbr] = name

    query_op = dbop()
    insert_sql='insert into state(name,abbreviation) values (%s,%s)'

    query_op.insert_database(insert_sql,rows)

    query_op.close_db()
    return abbr_state


def store_county_and_city():
    query_op = dbop()
    sql = 'select id,name from state'
    state_id = {}
    sid_state = {}
    for sid,name in query_op.query_database(sql):
        state_id[name] = sid
        sid_state[sid] = name

    stateset = set(state_id.keys())
    logging.info('number of states is {:}.'.format(len(state_id.keys())))

    ## 存储county 以及 city
    cf = open('data/us_zipcode_list.csv')
    cf.readline()
    state_county_city = defaultdict(lambda:defaultdict(list))
    for line in cf:
        line = line.strip()
        splits = line.split(',')
        state,city,county = splits[0],splits[2],splits[3]

        ## 如果不在53个州以内
        if state not in stateset:
            continue

        state_county_city[state][county].append(city)

    logging.info('there are {:} states in state county relations.'.format(len(state_county_city.keys())))

    county_rows = []
    city_rows = []
    for state in state_county_city.keys():
        sid = state_id[state]
        for county in state_county_city[state].keys():
            county_rows.append([county,sid])
            for city in set(state_county_city[state][county]):
                city_rows.append([sid,county,city])
    ## 存储county
    insert_sql = 'insert into county(name,sid) values (%s,%s)'
    query_op.insert_database(insert_sql,county_rows)

    ## 读取county的id
    county_id = {}
    sql = 'select id,name,sid from county'
    sid_county_id = defaultdict(dict)
    for cid,county,sid in query_op.query_database(sql):
        sid_county_id[sid][county] = cid

    new_city_rows = []
    for sid,county,city in city_rows:
        new_city_rows.append([sid,sid_county_id[sid][county],city])

    ## 存储city
    insert_sql = 'insert into city(sid,ctid,name) values (%s,%s,%s)'
    query_op.insert_database(insert_sql,new_city_rows)

    ## 读取city的id
    countyid_sid = {}
    cityid_sid = {}
    cityid_countyid = {}
    ## 从city中读取state county city之间的关系
    sql = 'select id,ctid,sid from city'
    for cityid,countyid,sid in query_op.query_database(sql):
        countyid_sid[countyid] = sid
        cityid_countyid[cityid] = countyid
        cityid_sid[cityid] = sid


    ## state county
    insert_sql = 'insert into state_county(cid,sid) values (%s,%s)'
    new_rows = []
    for countyid in countyid_sid.keys():
        new_rows.append([countyid,countyid_sid[countyid]])
    query_op.insert_database(insert_sql,new_rows)


    ## state city
    insert_sql = 'insert into state_city(cid,sid) values (%s,%s)'
    new_rows = []
    for cityid in cityid_sid.keys():
        new_rows.append([cityid,cityid_sid[cityid]])
    query_op.insert_database(insert_sql,new_rows)

    ## county city
    insert_sql = 'insert into county_city(cid,ctid) values (%s,%s)'
    new_rows = []
    for cityid in cityid_countyid.keys():
        new_rows.append([cityid,cityid_countyid[cityid]])
    query_op.insert_database(insert_sql,new_rows)

    query_op.close_db()

def store_ye():
    logging.info('storing youreconomy data ...')
    cf = open('data/county_add_missing_data.txt')
    titles = cf.readline().strip().split("\t")[4:]

    cols=[]
    for title in titles:
        col = title.split(':')[0].replace('-','').lower()
        if col=='change':
            col='changes'
        cols.append(col)
    cols = list(set(cols))
    state_county_year_data = defaultdict(lambda:defaultdict(dict))
    for line in cf:
        line = line.strip()
        splits = line.split('\t')

        year = int(splits[0])
        county = splits[1][:splits[1].index('(')].strip().lower()
        state = splits[2]

        state_county_year_data[state][county][year] = splits[4:]

    query_op = dbop()

    sql = 'select id,name,abbreviation from state'
    state_id = {}
    abbr_id = {}
    for sid,name,abbreviation in query_op.query_database(sql):
        state_id[name] = sid
        abbr_id[abbreviation] = sid

    sql = 'select id,name,sid from county'
    sid_county_id = defaultdict(dict)
    for cid,county,sid in query_op.query_database(sql):
        county = county.lower().replace(' city','').strip()
        sid_county_id[sid][county] = cid


    ye_insert_sql = 'insert into ye(year,cid,sid,{:}) values(%s,%s,%s,{:})'.format(','.join(cols),','.join(['%s']*len(cols)))
    # print ye_insert_sql
    ##首先将数据存储到attr里面去
    attr_insert_sql = 'insert into attribute(cid,sid,year,toptype,subtype,value,percent) values (%s,%s,%s,%s,%s,%s,%s)'

    for state in state_county_year_data.keys():
        logging.info('Save state {:} ...'.format(state))
        if state in state_id.keys():
                sid = state_id[state]
        else:
            if state in abbr_id.keys():
                sid = abbr_id[state]
            else:
                print 'error',state
                continue

        for county in state_county_year_data[state].keys():

            # print sid_county_id[state_id]
            county = county.lower().replace('st.','saint').replace(' city','').strip()
            cid = sid_county_id[sid].get(county.lower(),-1)

            if cid==-1:
                
                logging.info('Error county {:}-{:}.'.format(county,state))
                continue



            for year in state_county_year_data[state][county].keys():
                # logging.info('{:},{:},{:}....'.format(state,county,year))
                attrs = state_county_year_data[state][county][year]
                col_id = {}
                col_subtype_attr_values = defaultdict(lambda:defaultdict(dict))
                ## 对于每一列的attrbute， 存储wei attr
                for i,attr in enumerate(attrs):
                    title = titles[i]
                    col = title.split(':')[0].replace('-','').lower()
                    if col=='change':
                        col = 'changes'

                    subtype = title.split(':')[1]
                    attrt = title.split(':')[2]
                    col_subtype_attr_values[col][subtype][attrt]=attr

                for col in col_subtype_attr_values.keys():
                    for subtype in col_subtype_attr_values[col].keys():
                        percent = col_subtype_attr_values[col][subtype]['attr_of_total']
                        value = col_subtype_attr_values[col][subtype]['attr_value']
                        ### 这里对于每一个属性都会存一个数值，导致了整个的一个问题，所以首先将attr所有都存起来，然后再存ye
                        row  = [cid,sid,year,col,subtype,value,percent]
                        query_op.batch_insert(attr_insert_sql,row,100000,is_auto=False,end=False)

    query_op.batch_insert(attr_insert_sql,None,100000,is_auto=False,end=True)
    query_op.close_db()


def store_indeed():
    ##State中读取state的简写
    logging.info('store indeed data ...')
    query_op = dbop()
    sql = 'select id,abbreviation from state'
    sid_abbr  = {}
    for sid,abbreviation in query_op.query_database(sql):
        sid_abbr[sid]=abbreviation

    ## 从city中读取state county city之间的关系
    sql = 'select id,name,sid from city'
    state_city_cid = defaultdict(dict)
    for cityid,name,sid in query_op.query_database(sql):
        abbr = sid_abbr[sid]
        city = name.strip().lower().replace(' ','')
        state_city_cid[abbr][city] = cityid

    path = 'data/indeed_data.txt'
    insert_sql = 'insert into job(jobid,company,cityid,position,postype,publishdate,salary) values (%s,%s,%s,%s,%s,%s,%s)'
    for line in open(path):
        line = line.strip()
        _id,company,city,state,publishdate,position,postype,salary=line.split('\t')
        if _id=='id':
            continue

        ## 根据city和state联立起来

        name = city.strip().lower().replace(' ','')
        cid = state_city_cid[state].get(name,-1)
        if cid==-1:
            logging.info('error city {:}=={:}'.format(city,state))
            continue


        row = [_id,company,cid,position,postype,publishdate,salary]

        query_op.batch_insert(insert_sql,row,5000,is_auto=False,end=False)

    query_op.batch_insert(insert_sql,row,5000,is_auto=False,end=True)


    query_op.close_db()
    logging.info('Done')




    


if __name__ == '__main__':
    label = sys.argv[1]
    if label=='store_state':
        store_state()
        store_county_and_city()

    elif label=='store_ye':
        store_ye()

    elif label=='store_indeed':
        store_indeed()

    else:
        logging.info('No such actions.')

