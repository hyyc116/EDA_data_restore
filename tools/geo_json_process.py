#coding:utf-8

import json
from db_util import *

def read_counties():

    query_op = dbop()
    sql = 'select sid,county.name,abbreviation from county,state where county.sid = state.id'

    state_county = defaultdict(list)
    for sid,name,abbreviation in query_op.query_database(sql):
        name = pro_county_name(name,abbreviation)
        state_county[abbreviation].append(name)

    geo_json = json.loads(open('/Users/huangyong/Downloads/us-counties.json').read())
    state_num = 0
    
    for state in geo_json['features']:
        abbr = state['properties']['state']
        num = int(state['properties']['counties'])
        if state_county.get(abbr,-1)==-1:
            continue
        # print abbr,num,len(set(state_county[abbr]))
        # if num!=len(set(state_county[abbr])):
        #     print abbr
        cts = set(state_county[abbr])
        state_num+=1
        counties = []
        for county in state['counties']:
            cname = county['name']
            cname = pro_county_name(cname,abbr)
            if cname not in cts:
                # county['name'] = cname
                print cname

    # open('/Users/huangyong/Downloads/us-counties-renamed.json','w').write(json.dumps(geo_json))

def pro_county_name(name,state):
    name = name.lower().replace(' city','').replace('sainte.','saint').replace('st.','saint').replace('st','saint').replace('-',' ').replace('\'','').strip()
    if name.startswith('de') or name.startswith('la'):
        name = name.replace(' ','')
    name = name.capitalize()
    return  name+"("+state+")"  


if __name__ == '__main__':
    read_counties()