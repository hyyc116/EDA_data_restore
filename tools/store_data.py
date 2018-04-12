#coding:utf-8
from db_util import *

def store_data():
    def store_state():
        ## store state data into database
        rows = []
        abbr_state = {}
        for line in open('state.txt'):
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

    def store_county():
        query_op = dbop()
        sql = 'select id,name from state'
        state_name_id = {}
        for sid,name in query_op.query_database(sql):
            state_name_id[name] = sid

        ## store county 
        cf = open('us_zipcode_list.csv')
        cf.readline()
        state_county = defaultdict(list)
        for line in cf:
            line = line.strip()
            splits = line.split(',')
            state,county = splits[0],splits[3]
            state_county[state].append(county)

        insert_sql = 'insert into county(name,sid) values (%s,%s)'
        rows=[]
        for state in state_county.keys():
            print state

            sid = state_name_id.get(state,-1)
            if sid==-1:
                continue
            counties = set(state_county[state])

            for county in counties:
                rows.append([county,sid])

        query_op.insert_database(insert_sql,rows)

        sql = 'select id,name,sid from county'

        state_county_id = defaultdict(dict)
        for cid,county,sid in query_op.query_database(sql):
            state_county_id[sid][county.lower()]=cid

        query_op.close_db()

        return state_name_id,state_county_id

    def store_ye(abbr_state,state_name_id,state_county_id):

        cf = open('county_add_missing_data.txt')

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
        ye_insert_op = dbop()
        ye_insert_sql = 'insert into ye(year,cid,{:}) values(%s,%s,{:})'.format(','.join(cols),','.join(['%s']*len(cols)))
        # print ye_insert_sql
        for state in state_county_year_data.keys():

            if state in state_name_id.keys():
                    state_id = state_name_id[state]
            else:
                if state in abbr_state.keys():
                    state_name = abbr_state[state]
                    state_id = state_name_id[state_name]
                else:
                    print 'error',state
                    continue

            for county in state_county_year_data[state].keys():

                # print state_county_id[state_id]
                cid = state_county_id[state_id].get(county,-1)
                if cid==-1:
                    continue


                for year in state_county_year_data[state][county].keys():
                    logging.info('{:},{:},{:}....'.format(state,county,year))

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
                            attr_insert_sql = 'insert into attribute(subtype,value,percent) values (%s,%s,%s)'
                            aid = query_op.insert_sql(attr_insert_sql,[subtype,value,percent])
                            col_id[col] = aid

                    values = [year,cid]
                    for col in cols:
                        values.append(col_id[col])
                    
                    ye_insert_op.batch_insert(ye_insert_sql,values,2000,is_auto=False,end=False)

        ye_insert_op.batch_insert(ye_insert_sql,None,2000,is_auto=False,end=True)

        query_op.close_db()
        ye_insert_op.close_db()

    abbr_state = store_state()
    state_name_id,state_county_id = store_county()
    store_ye(abbr_state,state_name_id,state_county_id)



if __name__ == '__main__':
    store_data()
