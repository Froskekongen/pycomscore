import os
import getpass
import requests
import datetime
import gzip
import boto
import smart_open




class S3FileWriter(object):
    def __init__(self,bucket):
        self.bucket=bucket
        self.outputdir=''

    def getfile_loc(self,url):
        local_filename =self.outputdir + url.split('/')[-1]
        r=requests.get(url,stream=True)
        with open(local_filename,'wb') as ff:
            for chunk in r.iter_content(chunk_size=512*1024):
                if chunk:
                    ff.write(chunk)
        return local_filename

    def getfile(self,url,key=None,nlines=1000):
        local_filename=self.getfile_loc(url)
        conn=boto.connect_s3()
        bucket=conn.create_bucket(self.bucket)
        if not key:
            key=bucket.get_key(local_filename.strip('gz'),validate=False)
        kname=key.name
        uri='s3://'+bucket.name+'/'+kname
        linelist=[]
        with gzip.open(local_filename,'r') as ff,smart_open.smart_open(uri,'wb') as kk:
            for line in ff:
                linelist.append(line)
                if len(linelist)%nlines==0:
                    kk.write(b''.join(linelist))
            if len(linelist)>0:
                kk.write(b''.join(linelist))










class StreamFileWriter(object):
    def __init__(self,outputdir='.'):
        self.outputdir=outputdir

    def getfile(self,url,key=None):
        local_filename =self.outputdir+'/' + url.split('/')[-1]
        if not key:
            key=local_filename[:-3]

        outfn=key
        if os.path.exists(outfn):
            raise OSError('File exists. Try to use it.','',outfn)

        r=requests.get(url,stream=True)
        with open(local_filename,'wb') as ff:
            for chunk in r.iter_content(chunk_size=512*1024):
                if chunk:
                    ff.write(chunk)
        #Decompressing newly created file

        with open(outfn,mode='w') as gg:
            with gzip.open(local_filename,'r') as ff:
                for line in ff:
                    gg.write(line.decode('utf-8'))





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
        url_to_get="""https://dax-rest.comscore.eu/v1/reportitems.xml?\
itemid={0}&startdate={1}&enddate={2}&site=amediatotal&format={5}&\
client=amedia&parameters=Page:*&user={3}&password={4}"""
        url=url_to_get.format(repnr,startdate,enddate,self.username,self.passwd,self.format)
        output=requests.get(url)
        print(output)
        return output

    def get_large_report(self,repnr,startdate,enddate,fileWriter,site='amediatotal'):
        url_to_get="""https://dax-rest.comscore.eu/v1/datadownloadreportitem.csv?\
itemid={0}&startdate={1}&site={6}&\
client=amedia&user={3}&password={4}&\
nrofrows=unlimited&enddate={2}&format={5}"""
        print(url_to_get)
        url=url_to_get.format(repnr,startdate,enddate,self.username,self.passwd,\
            self.format,site)
        link=requests.get(url).json()['url']
        fileWriter.getfile(link)
        return link



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
