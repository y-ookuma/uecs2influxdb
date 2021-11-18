#-------------------------------------------------------------------------------
# Name: Exec_Influxdb.py
# Purpose: Ubiquitous Environment Control System
#          UECS is Japanese Green house Control System.
#          UDP BROADCAST Capture & Send program
# referencd : https://uecs.jp/
# Author:      ookuma yousuke
#
# Created: 2019/12/25
# Copyright:   (c) ookuma 2019
# Licence:     MIT License（X11 License）
#-------------------------------------------------------------------------------
#!/usr/bin python3
# -*- coding: utf-8 -*-
from influxdb import InfluxDBClient,DataFrameClient
import sys,time,os,json
import Initial_set,configparser
import chk_process
from datetime import datetime
import logging

## Influxクラス
class Influx_db():
    def __init__(self):
        self.db_user = config['influx_db']['user_name']
        self.db_pass = config['influx_db']['user_pass']
        self.db_database = config['influx_db']['db_name']
    if host_name != config['influx_db']['local_host']:
        # cloud db set
        db_user = config['influx_db_cloud']['user_name']
        db_pass = config['influx_db_cloud']['user_pass']
        db_database = config['influx_db_cloud']['db_name']

    return db_user,db_pass,db_database

def insert_influxdb(config,host_name,json_body):
    db_user,db_pass,db_database = set_dbuser(config,host_name)
    client = InfluxDBClient(host_name, 8086, db_user, db_pass, db_database, timeout=3, retries=6 )

    try:
        client.write_points(json_body)
    except :
        pass

def delete_influxdb(config,host_name,table,script):
    db_user,db_pass,db_database = set_dbuser(config,host_name)
    client = InfluxDBClient(host_name, 8086, db_user, db_pass, db_database,timeout=3, retries=6)
    if script is None:
        script=( 'delete from \"%s\" where  sum = \'0\' and time < now()-24h; ' % (table))
    try:
        client.query(script)
    except :
        pass

def select_influxdb(config,host_name,table,func,script):
    db_user,db_pass,db_database = set_dbuser(config,host_name)
    client = InfluxDBClient(host_name, 8086, db_user, db_pass, db_database,timeout=3, retries=6)
    if script is None:
        interval = config['influx_db']['interval'] + 'm'
        script = ('SELECT %s as value FROM \"%s\" where time < now()-24h and sum = \'0\' GROUP BY time(%s); ' % (func,table,interval))
    try:
        result = client.query(script)
    except :
        pass
        return None
    return result

def downsampling(config,host_name,table,func):
    db_user,db_pass,db_database = set_dbuser(config,host_name)
    client = InfluxDBClient(host_name, 8086, db_user, db_pass, db_database,timeout=3, retries=6)
    r={}
    insert_data={}
    r = select_influxdb(config,host_name,table,func,None)
    for x in r:
        for i in range(0,len(x)):
#            print(x[i]['time'],x[i]['value'])
            if x[i]['value'] is not None:
                print(x[i]['time'],x[i]['value'])
                insert_influxdb(config,host_name,table,x[i]['value']*1.0,None,'1',x[i]['time'])
                delete_influxdb(config,host_name,table,None)
                print('downsampling !')

def cloud_data_up(config,FLG_UP):
    table_last_time = {}
    table_last_time = get_last_time(config,FLG_UP)
    local_table = FLG_UP
    if table_last_time is not None :
        local_table = list(set(FLG_UP)-set(list(table_last_time.keys())))

    for table in FLG_UP:
#    for table in table_last_time.keys():

        if table_last_time[table] is not None:
            script_A='select * from \"%s\" where time > \'%s\' order by time asc limit 1000;' % (table , table_last_time[table])
        else :
            script_A='select * from \"%s\" order by time asc limit 1000;' % (table)

        # Get upload data
#        print(script_A)
        r_local = select_influxdb(config,config['influx_db']['local_host'],table,None,script_A)
        data = list(r_local.get_points(measurement=table ))

        json_body = []
        for i in data:
            json_body.append({
                            "measurement": table,
                            "tags": {
                                "sum": i['sum']
                            },
                            "time": i['time'],
                            "fields": {
                                    "value": float(i['value'])*1.0
                         }
                    })

        host_name = config['influx_db_cloud']['cloud_host']
        db_user,db_pass,db_database = set_dbuser(config,host_name)
        client = InfluxDBClient(host_name, 8086, db_user, db_pass, db_database,timeout=3, retries=6)
        print("influxdb[%s]: %s" %(host_name,json_body))
        try:
            client.write_points(json_body)
        except :
            pass


def aggregate_influxdb(config,host_name,json_body):
    db_user,db_pass,db_database = set_dbuser(config,host_name)
    client = InfluxDBClient(host_name, 8086, db_user, db_pass, db_database,timeout=3, retries=6)

    print("influxdb(%s): %s" % (host_name,json_body))
    try:
        client.write_points(json_body)
    except :
        pass


def ABC_data(config,host_name,table,start,end):
    db_user,db_pass,db_database = set_dbuser(config,host_name)
    client = InfluxDBClient(host_name, 8086, db_user, db_pass, db_database,timeout=3, retries=6)

#    script = ('select time,mean(value) as value from %s where time > %s and time < %s  tz(\'Asia/Tokyo\')') % (table,start,end)
    script = ('select time,mean(value) as value from \"%s\" where time >= %s and time < %s ') % (table,start,end)
    r = select_influxdb(config,config['influx_db']['local_host'] ,'ABC_'+table,None,script)
    print(script)

    for x in r:
        for i in range(0,len(x)):
            if x[i]['value'] is not None:
                return x[i]['value']*1.0
            else:
                return None


def get_last_time(config,FLG_UP):
    script = 'SHOW MEASUREMENTS'
    r = select_influxdb(config,config['influx_db']['local_host'],None,None,script)
    if r is None:
        return None

    table_list=[]
    for x in r:
        for i in range(0,len(x)):
            table_list.append(x[i]['name'])

    table_list.extend(FLG_UP)
    li_uniq = list(set(table_list))

    script_B=''
    table_last_time = {}

    for table in li_uniq:
        script_B = script_B + ' select last(*) from \"%s\"; ' % (table)
        table_last_time[table] = None

    r = select_influxdb(config,config['influx_db_cloud']['cloud_host'],None,None,script_B)
    if r is None:
        return table_last_time

    for x in r:
        key = str(x).split(':')[0]
        table = key.replace("ResultSet({\'(\'","").split('\'')[0]
        for s in x:
            table_last_time[table] = s[0]['time']

#    print(table_last_time)
    return table_last_time

