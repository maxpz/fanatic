import riak
import xml.etree.ElementTree as ET
import time
from urllib.request import urlopen
import math
starttime=time.time()
import csv
import requests

cont = 0
search_index = 'grape_lead_index'
pageSize = 2000

#connection = urlopen('https://devlb.spartanapproach.com/search/query/{}?wt=json&q=uuid_register:*&rows={}&sort=score+desc,_yz_id+asc&cursorMark=*'.format(search_index, pageSize))
connection = urlopen('https://devlb.spartanapproach.com/search/query/grape_lead_index?wt=json&q=uuid_register:*&fq=((health_lead_status_register:%222018%20welcome%20call%22)OR(health_lead_status_register:%22welcome%20call%22)OR(health_lead_status_register:%22tentative%22)OR(health_lead_status_register:%22pending%22)OR(health_lead_status_register:%22presold%22)OR(health_lead_status_register:%22enrolled%22))&rows={}&sort=score+desc,_yz_id+asc&cursorMark={}'.format(pageSize,'*'))
response = eval(connection.read())
numFound = response['response']['numFound']
nextCursor = response['nextCursorMark']

if numFound > pageSize:
    j = math.ceil(numFound/pageSize)-1
else:
    j = 1


dUUID = {}

while (j>0):
    for item in response['response']['docs']:
        key = item['_yz_rk']
        print("UUID: {}".format(item["uuid_register"]))
        if item['uuid_register'] not in dUUID:
            line = []
            dUUID[item['uuid_register']] = 1
            #row = bucket.get(key)    # liono
            #if (row is not None):
            cont += 1
            print("{}) - Processing key: {}".format(cont,key))
            url = 'https://devlb.spartanapproach.com/leads/{}'.format(item['uuid_register'])
            payload = '{{"status":"{}"}}'.format("lawyer")
            headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
            r = requests.patch(url, data=payload, headers=headers)
            print(r)
    print("tick")
    time.sleep(2.0 - ((time.time() - starttime) % 2.0))
    connection = urlopen('https://devlb.spartanapproach.com/search/query/grape_lead_index?wt=json&q=uuid_register:*&fq=((health_lead_status_register:%222018%20welcome%20call%22)OR(health_lead_status_register:%22welcome%20call%22)OR(health_lead_status_register:%22tentative%22)OR(health_lead_status_register:%22pending%22)OR(health_lead_status_register:%22presold%22)OR(health_lead_status_register:%22enrolled%22))&rows={}&sort=score+desc,_yz_id+asc&cursorMark={}'.format(pageSize,nextCursor))
    #connection = urlopen('https://devlb.spartanapproach.com/search/query/{}?wt=json&q=uuid_register:*&rows={}&sort=score+desc,_yz_id+asc&cursorMark={}'.format(search_index, pageSize,nextCursor))
    response = eval(connection.read())
    nextCursor = response['nextCursorMark']
    j-=1
