from pyzabbix import ZabbixAPI
import json
import pymssql as sql

# file with credentials
data = json.loads(open('auth2.json').read())

# login/pass for zabbix
zabbix_username = data['zabbix_username']
zabbix_password = data['zabbix_password']

# login/pass for sql
agent_username = data['agent_username']
agent_password = data['agent_password']

# central management server name
servname = r'<central management server name>'

"""
    queries for sql
    query - to get server list from central management server
    query1 - to get disks letters from sql, where database files's storing
"""
query = "select REPLACE(server_name, '\', '/') from sysmanagement_shared_registered_servers where server_name like '<optional condition>'"
query1 = """
    select SUBSTRING(filename, 1, 2) from sys.sysaltfiles where dbid != 32767
    group by SUBSTRING(filename, 1, 2)
"""
# var for macros name
macro = '{$DISKROOT}'
# var for some hosts group id
groupid = 130
# dict for servername:drives_letters
slist = {}

def zabbix_connect(url='<zabbix address>', user=zabbix_username, password=zabbix_password):
    zapi = ZabbixAPI(url=url, user=user, password=password)
    return zapi
def createMacro(vHostID, vValue, macro):
    zbx = zabbix_connect()
    zbx.usermacro.create({'hostid': vHostID, 'macro': macro, 'value': vValue})
def updateMacro(vMacroID, vValue, macro):
    zbx = zabbix_connect()
    zbx.usermacro.update({'hostmacroid': vMacroID, 'macro': macro, 'value': vValue})
def getMacro(vHostID, macro):
    zbx = zabbix_connect()
    some = zbx.usermacro.get(hostids=vHostID, filter={"macro": [macro]})
    return some

# function for sql connect
def conn(servname, query, dbname = "msdb"):

    server = servname
    user = agent_username
    password = agent_password

    con = sql.connect(server, user, password, dbname)
    cursor = con.cursor()
    cursor.execute(query)
    row = cursor.fetchone()
    f = []
    while row:
        f.append(row[0].upper())
        row = cursor.fetchone()

    con.close()

    return f

# get hosts from some group
zbx = zabbix_connect()
hosts = zbx.host.get(
    groupids=[groupid]
)

# create server list from central management server name
servlist = conn(servname=servname, query=query)

# filling dict server:drives_letters
for i in servlist:
    try:
        slist[i.replace('\\', '/')] = '|'.join(conn(servname=i, query=query1, dbname="master"))
        continue
    except ValueError:
        print(i)

# main work
for host in hosts:
    if len(host['name']) > 13 and host['name'] in slist:
        if len(getMacro(vHostID=host['hostid'], macro=macro)) > 0:
            print(host['name'] + ' already')
        else:
            createMacro(vHostID=host['hostid'], macro=macro, vValue=slist[host['name']])
            print(host['name'] + ' done')
