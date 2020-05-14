# -*- coding: utf-8 -*-
"""
Created on Mon Apr 13 16:29:34 2020

@author: Irene López-Rodríguez
"""

from sqlalchemy import create_engine
import pandas as pd
import os
import sys

################################################################################
#  >  QUERY TO POSTGRESQL DATABASE
################################################################################

def get_atc_icd_data(conn):
    SQL_query = """select c.code, ac.code, ac.name
                    from commonnamegroup cng 
                    inner join commonnamegroup_indication cngi on cngi.commonnamegroupid = cng.commonnamegroupid
                    inner join indicationgroup_indication igi on igi.indicationid = cngi.indicationid
                    inner join cim10_indicationgroup cig on cig.indicationgroupid = igi.indicationgroupid
                    inner join cim10 c on c.cim10id = cig.cim10id
                    inner join commonnamegroup_atc cnga on cnga.commonnamegroupid = cng.commonnamegroupid
                    inner join atcclass ac on ac.atcclassid = cnga.atcclassid
                    group by c.code, ac.code, ac.name order by c.code asc
                    ;"""
    df = pd.read_sql_query(SQL_query, conn)
    df.to_csv('data_with_atc_icd_codes.csv', index = False, encoding = 'utf-8-sig')
    return df

def get_sideeffects_atc_data(conn):
    SQL_query = """select se.sideeffectid, se.name, ac.code, ac.name
                    from commonnamegroup cng 
                    inner join commonnamegroup_indication cngi on cngi.commonnamegroupid = cng.commonnamegroupid
                    inner join commonnamegroup_sideeffect cngs on cngs.commonnamegroupid = cngi.commonnamegroupid
                    inner join sideeffect se on se.sideeffectid = cngs.sideeffectid
                    inner join commonnamegroup_atc cnga on cnga.commonnamegroupid = cng.commonnamegroupid
                    inner join atcclass ac on ac.atcclassid = cnga.atcclassid
                    group by se.sideeffectid, se.name, ac.code, ac.name
                    ;"""
    df = pd.read_sql_query(SQL_query, conn)
    df.to_csv('data_with_atc_sideeffects.csv', index = False, encoding = 'utf-8-sig')
    return df

################################################################################
#  >  CONNECTION SETUP POSTGRESQL DATABASE
################################################################################
def init_db(database='local'):
    """
    OPTIONS:
        local :: LOCAL DATABASE
    """
    global dbengine, conn

    # __SQL CONNECTION SETUP__
    if database == 'local':
        user = "username"
        password = "*******"
        database = "dbname"
        host = "localhost"


    dbengine = create_engine(f'postgresql://{user}:{password}@{host}/{database}',
                             echo=False)
    conn = dbengine.connect()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        flag = sys.argv[1]
    else:
        flag = 0
        print("Give one option to create the datasets: 1-PostgreSQL or 2-Pandas")

################################################################################
#  >  LOAD DATA TO POSTGRESQL DATABASE AND QUERY RESULTS
################################################################################       
    if flag == "1":
        init_db('local')
        
        entries = os.listdir('latam_csv/')
        for entry in entries:
            data = pd.read_csv('latam_csv/' + entry, encoding = 'utf-8-sig')
            data.to_sql(entry.split('.', 2)[0], dbengine, index=False, if_exists='append')
        
        get_atc_icd_data(conn)
        get_sideeffects_atc_data(conn)
        conn.close()
        
################################################################################
#  >  COMBINE DATA USING PANDAS INNER JOINS WITHOUT POSTGRESQL DATABASE 
################################################################################    
    elif flag == "2":
    #   ATC AND CIE    
        commonnamegroup = pd.read_csv('latam_csv/commonnamegroup.csv', encoding = 'utf-8-sig')
        commonnamegroup_indication = pd.read_csv('latam_csv/commonnamegroup_indication.csv', encoding = 'utf-8-sig')
        merged_inner = pd.merge(left=commonnamegroup, right=commonnamegroup_indication, left_on='commonnamegroupid', right_on='commonnamegroupid')
        
        indicationgroup_indication = pd.read_csv('latam_csv/indicationgroup_indication.csv', encoding = 'utf-8-sig')
        merged_inner = pd.merge(left=merged_inner, right=indicationgroup_indication, left_on='indicationid', right_on='indicationid')
        
        cim10_indicationgroup = pd.read_csv('latam_csv/cim10_indicationgroup.csv', encoding = 'utf-8-sig')
        merged_inner = pd.merge(left=merged_inner, right=cim10_indicationgroup, left_on='indicationgroupid', right_on='indicationgroupid')
        
        cim10 = pd.read_csv('latam_csv/cim10.csv', encoding = 'utf-8-sig')
        merged_inner = pd.merge(left=merged_inner, right=cim10, left_on='cim10id', right_on='cim10id')
        
        commonnamegroup_atc = pd.read_csv('latam_csv/commonnamegroup_atc.csv', encoding = 'utf-8-sig')
        merged_inner = pd.merge(left=merged_inner, right=commonnamegroup_atc, left_on='commonnamegroupid', right_on='commonnamegroupid')
        
        atcclass = pd.read_csv('latam_csv/atcclass.csv', encoding = 'utf-8-sig')
        merged_inner = pd.merge(left=merged_inner, right=atcclass, left_on='atcclassid', right_on='atcclassid')
        
        merged_inner.shape
        merged_inner.to_csv('atc_icd_pairs.csv', index = False, encoding = 'utf-8-sig')
        
    #   SIDE EFFECTS    
        merged_inner_se = pd.merge(left=commonnamegroup, right=commonnamegroup_indication, left_on='commonnamegroupid', right_on='commonnamegroupid')
        
        commonnamegroup_sideeffect = pd.read_csv('latam_csv/commonnamegroup_sideeffect.csv', encoding = 'utf-8-sig')
        merged_inner_se = pd.merge(left=merged_inner_se, right=commonnamegroup_sideeffect, left_on='commonnamegroupid', right_on='commonnamegroupid')
        
        sideeffect = pd.read_csv('latam_csv/sideeffect.csv', encoding = 'utf-8-sig')
        merged_inner_se = pd.merge(left=merged_inner_se, right=sideeffect, left_on='sideeffectid', right_on='sideeffectid')
        merged_inner_se = pd.merge(left=merged_inner_se, right=commonnamegroup_atc, left_on='commonnamegroupid', right_on='commonnamegroupid')
        merged_inner_se = pd.merge(left=merged_inner_se, right=atcclass, left_on='atcclassid', right_on='atcclassid')
    
        merged_inner_se.shape
        merged_inner_se.to_csv('atc_sideeffects_pairs.csv', index = False, encoding = 'utf-8-sig')
    else:
        print("The given number is not an option!")
