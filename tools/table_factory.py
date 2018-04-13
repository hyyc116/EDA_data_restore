#coding:utf-8
from db_util import dbop



def create_tables():
    ## create table state
    query_op = dbop()
    state_sql = '''
        DROP DATABASE EDA_DATA;
        CREATE DATABASE `EDA_DATA` DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;
        USE EDA_DATA;
        DROP TABLE IF EXISTS `state`;
        '''

    for line in state_sql.split('\n'):
        if line.strip()=='':
            continue
        query_op.execute_sql(line)

    state_sql='''
        create table `state`(
            `id` int(10) NOT NULL AUTO_INCREMENT,
            `name` varchar(30) NOT NULL,
            `abbreviation` varchar(30) NOT NULL,
            PRIMARY KEY(`id`)
        );
        '''
    query_op.execute_sql(state_sql)

    county_sql = '''
        create table `county`(
            `id` int(10) NOT NULL AUTO_INCREMENT,
            `name` varchar(30) NOT NULL,
            PRIMARY KEY(`id`)
        );
        '''
    query_op.query_database(county_sql)


    city_sql = '''
        create table `city`(
            `id` int(10) NOT NULL AUTO_INCREMENT,
            `name` varchar(30) NOT NULL,
            PRIMARY KEY(`id`)
        );
        '''
    query_op.query_database(city_sql)

    ### relations

    ## state_county
    sc_sql = '''
        create table `state_county`(
            `id` int(10) NOT NULL AUTO_INCREMENT,
            `sid` int(10) NOT NULL,
            FOREIGN KEY (`sid`) REFERENCES state(`id`),
            `cid` int(10) NOT NULL,
            FOREIGN KEY (`cid`) REFERENCES county(`id`),
            PRIMARY KEY(`id`)
        );
        '''
    query_op.query_database(sc_sql)

    cs_sql = '''
        create table `county_state`(
            `id` int(10) NOT NULL AUTO_INCREMENT,
            `cid` int(10) NOT NULL,
            FOREIGN KEY (`cid`) REFERENCES county(`id`),
            `sid` int(10) NOT NULL,
            FOREIGN KEY (`sid`) REFERENCES state(`id`),
            PRIMARY KEY(`id`)
        );
        '''
    query_op.query_database(cs_sql)

    ## city state
    cs_sql = '''
        create table `city_state`(
            `id` int(10) NOT NULL AUTO_INCREMENT,
            `cid` int(10) NOT NULL,
            FOREIGN KEY (`cid`) REFERENCES city(`id`),
            `sid` int(10) NOT NULL,
            FOREIGN KEY (`sid`) REFERENCES state(`id`),
            PRIMARY KEY(`id`)
        );
        '''
    query_op.query_database(cs_sql)

    ## state city
    cs_sql = '''
        create table `state_city`(
            `id` int(10) NOT NULL AUTO_INCREMENT,
            `sid` int(10) NOT NULL,
            FOREIGN KEY (`sid`) REFERENCES state(`id`),
            `cid` int(10) NOT NULL,
            FOREIGN KEY (`cid`) REFERENCES city(`id`),
            PRIMARY KEY(`id`)
        );
        '''
    query_op.query_database(cs_sql)

    ## city county
    cs_sql = '''
        create table `city_county`(
            `id` int(10) NOT NULL AUTO_INCREMENT,
            `cid` int(10) NOT NULL,
            FOREIGN KEY (`cid`) REFERENCES city(`id`),
            `ctid` int(10) NOT NULL,
            FOREIGN KEY (`ctid`) REFERENCES county(`id`),
            PRIMARY KEY(`id`)
        );
        '''
    query_op.query_database(cs_sql)

    ## county city
    cs_sql = '''
        create table `county_city`(
            `id` int(10) NOT NULL AUTO_INCREMENT,
            `ctid` int(10) NOT NULL,
            FOREIGN KEY (`ctid`) REFERENCES county(`id`),
            `cid` int(10) NOT NULL,
            FOREIGN KEY (`cid`) REFERENCES city(`id`),
            PRIMARY KEY(`id`)
        );
        '''
    query_op.query_database(cs_sql)

    attrs_sql = '''
        create table `attribute`(
            `id` int(10) NOT NULL AUTO_INCREMENT,
            `subtype` varchar(50) NOT NULL,
            `value` varchar(50) NOT NULL,
            `percent` varchar(50) NOT NULL,
            PRIMARY KEY(`id`)
        );
        '''
    query_op.query_database(attrs_sql)
        
    ye_sql = '''
        create table `ye`(
            `id` int(10) NOT NULL AUTO_INCREMENT,
            `year` int(4) NOT NULL,
            `allsales` int(10) DEFAULT NULL,
            FOREIGN KEY (`allsales`) REFERENCES attribute(`id`),
            `totalgained` int(10) DEFAULT NULL,
            FOREIGN KEY (`totalgained`) REFERENCES attribute(`id`),
            `salesperestablishment` int(10) DEFAULT NULL,
            FOREIGN KEY (`salesperestablishment`) REFERENCES attribute(`id`),
            `allbusinesses` int(10) DEFAULT NULL,
            FOREIGN KEY (`allbusinesses`) REFERENCES attribute(`id`),
            `nonresidentbusinesses` int(10) DEFAULT NULL,
            FOREIGN KEY (`nonresidentbusinesses`) REFERENCES attribute(`id`),
            `totallost` int(10) DEFAULT NULL,
            FOREIGN KEY (`totallost`) REFERENCES attribute(`id`),
            `salesperemployee` int(10) DEFAULT NULL,
            FOREIGN KEY (`salesperemployee`) REFERENCES attribute(`id`),
            `alljobs` int(10) DEFAULT NULL,
            FOREIGN KEY (`alljobs`) REFERENCES attribute(`id`),
            `noncommercial` int(10) DEFAULT NULL,
            FOREIGN KEY (`noncommercial`) REFERENCES attribute(`id`),
            `residentbusinesses` int(10) DEFAULT NULL,
            FOREIGN KEY (`residentbusinesses`) REFERENCES attribute(`id`),
            `changes` int(10) DEFAULT NULL,
            FOREIGN KEY (`changes`) REFERENCES attribute(`id`),
            PRIMARY KEY(`id`)
        );
        '''
    query_op.query_database(ye_sql)

    ## county ye
    cy_sql = '''
        create table `c_ye`(
            `id` int(10) NOT NULL AUTO_INCREMENT,
            `cid` int(4) NOT NULL,
            FOREIGN KEY (`cid`) REFERENCES county(`id`),
            `yid` int(10) NOT NULL,
            FOREIGN KEY (`yid`) REFERENCES ye(`id`),
            PRIMARY KEY(`id`)
        );
        '''
    query_op.query_database(cy_sql)

    job_sql = '''
        create table `job`(
            `id` int(10) NOT NULL AUTO_INCREMENT,
            `title` varchar(30) NOT NULL,
            `company` varchar(10) NOT NULL,
            `position` varchar(10) NOT NULL,
            `summary` text DEFAULT NULL,
            `publishdate` varchar(30) NOT NULL,
            `salary` int(20) DEFAULT -1,
            `path` varchar(10) NOT NULL,
            PRIMARY KEY(`id`)
        );
        '''
    query_op.query_database(job_sql)

    skill_sql = '''
        create table `skill`(
            `id` int(10) NOT NULL AUTO_INCREMENT,
            `name` varchar(50) NOT NULL,
            PRIMARY KEY(`id`)
        );
        '''
    query_op.query_database(skill_sql)

    job_skill = '''
        create table `relations`(
            `id` int(10) NOT NULL AUTO_INCREMENT,
            `jid` int(10) NOT NULL,
            `skillid` int(10) NOT NULL,
            FOREIGN KEY (`jid`) REFERENCES job(`id`),
            FOREIGN KEY (`skillid`) REFERENCES skill(`id`),
            PRIMARY KEY(`id`)
        );
        '''
    query_op.query_database(job_skill)

    job_city_sql = '''
        create table `job_city`(
            `id` int(10) NOT NULL AUTO_INCREMENT,
            `jid` int(10) NOT NULL,
            `cid` int(10) NOT NULL,
            FOREIGN KEY (`jid`) REFERENCES job(`id`),
            FOREIGN KEY (`cid`) REFERENCES city(`id`),
            PRIMARY KEY(`id`)
        );
        '''
    query_op.query_database(job_city_sql)

    query_op.close_db()


if __name__ == '__main__':
    create_tables()


