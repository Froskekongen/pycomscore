import os
import getpass
import requests
import datetime


class GetReports(object):

    def __init__(self,confdir=os.environ['HOME']+'/.pycomscore'):
        self.confdir=confdir
        try:
            self.get_config()
        except:
            self.configure()

    def get_config(self):
        configfp=self.confdir+'/credentials.conf'
        if os.path.exists(configfp):
            with open(configfp) as ff:
                self.username=ff.readline().strip()
                self.passwd=ff.readline().strip()
                self.format=ff.readline().strip()
        else:
            raise('Configfile {0} not found'.format(configfp))

    def configure(self):
        os.makedirs(self.confdir)
        print('Configuration folder, {0}, created'.format(self.confdir))
        configfp=self.confdir+'/credentials.conf'
        with open(configfp,mode='w') as ff:
            username=input('Username: ')
            password=getpass.getpass()
            gf=input('Format: ')
            ff.write(username+'\n')
            ff.write(password+'\n')
            ff.write(gf+'\n')
        self.get_config()

    def get_report(self,repnr,startdate,enddate):
        url_to_get="""https://dax-rest.comscore.eu/v1/reportitems.xml?itemid={0}&startdate={1}&enddate={2}&site=amediatotal&format={5}&client=amedia&parameters=Page:*&user={3}&password={4}"""
        url=url_to_get.format(repnr,startdate,enddate,self.username,self.passwd,self.format)
        output=requests.get(url)
        print(output)
        return output


class ParseComscoreJson(object):
    def __init__(self):
        self.convdict={'Day':self.pdate,'a_virtual':str,'Week':self.pweek}

    def pdate(self,stdate):
        return datetime.datetime.strptime(stdate,'%d-%m-%Y')

    def pweek(self,weekstr):
        spl=weekstr.split()
        ww=tuple(int( s) for s in spl[1:])[::-1]
        return ww


    def parse_json_report(self,jsonreport):
        cols=jsonreport['reportitems']['reportitem'][0]['columns']
        cdict={}
        for iii,col in enumerate(cols['column']):
            title=col['ctitle']
            if col['type']=='integer':
                cdict[iii]=[title,int]
            else:
                try:
                    cdict[iii]=[title,self.convdict[title]]
                except Exception as e:
                    print('Type not found. Using string',e)
                    cdict[iii]=[title,str]

        dictlist=[]
        for row in jsonreport['reportitems']['reportitem'][0]['rows']['r']:
            if 'Total' in set(row['c']):
                continue
            dd={}
            for iii,r in enumerate(row['c']):
                dd[cdict[iii][0]]=cdict[iii][1](r)
            dictlist.append(dd)
        return dictlist
