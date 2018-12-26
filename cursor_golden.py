import riak
import xml.etree.ElementTree as ET
import time
from urllib.request import urlopen
import math
starttime=time.time()
import csv

cont = 0
search_index = 'grape_lead_index'
#path_index = '/home/sparta_max/code/lemon/lemon/data/riak/search/schemas/{}.xml'.format(search_index)
path_index = '/var/lib/riak/yz/{}/conf/{}.xml'.format(search_index, search_index)
bucket_type = 'grape_lead'
bucket_name = 'leads'
pageSize = 2000

#download_dir = "export_queryAll_{}.csv".format(bucket_name)
download_dir = "grape_lead_index_NOT.csv"
csv_file = open(download_dir, "w")
writer = csv.writer(csv_file, dialect='excel')


#Read Index, to know which fields to export
tree = ET.parse(path_index)
root = tree.getroot()
fields = []
for child in root[0]:
    #Exclude special fields, and default ones. Just leave the ones we care.
    if(child.tag == "field" and not (child.attrib["name"].startswith('_')) and not (child.attrib["name"].startswith('*'))):
        fields.append(child.attrib["name"].split('_register')[0])

#Write CSV Header
writer.writerow(fields)


#connection = urlopen('https://devlb.spartanapproach.com/search/query/{}?wt=json&q=uuid_register:*&rows={}&sort=score+desc,_yz_id+asc&cursorMark=*'.format(search_index, pageSize))
connection = urlopen('https://devlb.spartanapproach.com/search/query/grape_lead_index?wt=json&q=uuid_register:*&fq=NOT((health_lead_status_register:%222018%20welcome%20call%22)AND(lead_source_register:boberdoo)AND(state_register:florida))&rows={}&sort=score+desc,_yz_id+asc&cursorMark=*'.format(pageSize))
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
            for i in fields:
                if '{}_register'.format(i) in item:
                    line.append(str(item[('{}_register'.format(i))]).encode("utf-8"))
                    #aux = aux + '"{}",'.format(str(item[('{}_register'.format(i))]).replace(',','~').replace('"','~').replace('\'','~'))
                else:
                    line.append("")
            writer.writerow(line)
    print("tick")
    time.sleep(2.0 - ((time.time() - starttime) % 2.0))
    connection = urlopen('https://devlb.spartanapproach.com/search/query/{}?wt=json&q=uuid_register:*&rows={}&sort=score+desc,_yz_id+asc&cursorMark={}'.format(search_index, pageSize,nextCursor))
    response = eval(connection.read())
    nextCursor = response['nextCursorMark']
    j-=1

csv_file.close()

