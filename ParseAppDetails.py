__author__ = 'purejade'

import requests

app_url = ''


import urllib
import urllib2
import re
import threading
import json
import Queue
import time
import os
import random

prefix = 'http://apkleecher.com/'
taskQueue = Queue.Queue(-1)

if  2==2:
    APP_DIR = 'G:'+os.sep+'FtpDir'+os.sep+'PLAY_APP'+os.sep
else:
    APP_DIR = 'G:'+os.sep+'FtpDir'+os.sep+'tmp'+os.sep

lost_app_file = open(APP_DIR+'lost_app_file','a+')
finished_app_file = open(APP_DIR+'finished_app_file','a+')
exception_app_file = open(APP_DIR+'exception_app_file','a+')

APP_MAP = {}

def write_page(name,content):
    open(name,'wb').write(content)

def append_page(filename,line):
    filename.write(line)
    filename.write(os.linesep)

HEADERS = {
    'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'zh-cn,zh;q=0.8,en-us;q=0.5,en;q=0.3',
    'Accept-Encoding': 'gzip, deflate',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0',
    'Host': 'apkleecher.com',
    'Connection': 'keep-alive'
}

class GoogleDownloader(threading.Thread):
    finished = 0
    def __init__(self,name):
        threading.Thread.__init__(self)
        self.session = requests.session()
        self.name = name
        self.daemon = True

    def run(self):
        if taskQueue.empty():
            return
        while taskQueue.empty()  == False:
            try:
                del self.session
                self.session = requests.session()
                url = 'http://apkleecher.com/'
                id_value = taskQueue.get()
                # if os.path.exists(APP_DIR+id_value+'.apk'):
                if APP_MAP.has_key(id_value):
                    taskQueue.task_done()
                    continue
                self.download(url,id_value)
                taskQueue.task_done()
            except Exception as e:
                append_page(lost_app_file,id_value)
                taskQueue.task_done()


    def download(self,url,id_value):
        proxies = {
            'http': 'http://127.0.0.1:8087',
            'https': 'http://127.0.0.1:8087',
        }
        print 'downloading the ' + id_value
        try:
            resp = self.session.get(url,timeout = 100,proxies=proxies, verify=False)
            if not resp:
                append_page(lost_app_file,id_value)
                print 'wrong download ' + id_value
                return

                # write_page('index.html',resp.content)
                resp = self.session.get(prefix+'?id='+id_value,timeout = 100,proxies=proxies, verify=False)
            # write_page('down.html',resp.content)
            if not resp:
                # print 'can not find the ' + str(id_value) + 'first graph'
                append_page(lost_app_file,id_value)
                print 'wrong download ' + id_value
                return
            if 'This app might be incompatible with our downloader' in resp.content:
                # print   id_value + 'This app might be incompatible with our downloader'
                print 'wrong download ' + id_value
                lost_app_file.write(id_value)
                lost_app_file.write(os.linesep)
                return
            content = resp.content
            index = content.find('Proceed to download page')
            content = content[index-100:index]
            loader_dl_object = re.search(r'href="(.*?)">',content)
            if not loader_dl_object:
                append_page(lost_app_file,id_value)
                print 'wrong download ' + id_value
                return
            loader_dl = loader_dl_object.group(1)
            # print loader_dl
            resp = self.session.get('http://apkleecher.com/'+loader_dl[2:],timeout = 100,proxies=proxies, verify=False)
            # write_page('downapk.html',resp.content)
            self.session.headers.update(HEADERS)
            if not resp:
                # print 'can not find the ' + str(id_value) + 'second graph'
                print 'wrong download ' + id_value
                append_page(lost_app_file,id_value)
                return
            loader_url_object = re.search(r'setTimeout\(\'location.href="\.\.\/\.\.\/(.*?)"\',25000\);',resp.content)
            if not loader_url_object:
                loader_url_object = re.search(r'setTimeout\(\'location.href="\.\.\/(.*?)"\',25000\);',resp.content)
            if not loader_url_object:
                # print id_value
                # print 'not exist two'
                append_page(lost_app_file,id_value)
                # print content
                print 'wrong download ' + id_value
                return
            loader_url = loader_url_object.group(1)
            # print loader_url
            # self.session.headers['referer'] = 'http://apkleecher.com/download/?dl=com.lego.starwars.theyodachronicles'
            try:
                resp = self.session.get('http://apkleecher.com/'+loader_url,timeout = 100,proxies=proxies, verify=False)
            except requests.exceptions.Timeout as timeout:
                print str(timeout)
                return
            if resp.status_code != 200 or not resp:
                append_page(lost_app_file,id_value)
                # print id_value
                # print content
                print 'wrong download ' + id_value
                return
            with open(APP_DIR+id_value+'.apk','wb') as f:   # write by binary format not ascii
                f.write(resp.content)
                self.finished = self.finished + 1
                append_page(finished_app_file,id_value)
                print id_value + ' is over!'
        except Exception as t:
            print str(t)
            return
        # time.sleep(3)

def CreateDataFromJson():
    playstore_handler = open('playstore.json','rb')
    k = 1
    limit = 20000
    start = random.randint(13000,20000)
    print 'loading data'
    for line in playstore_handler:
        lineObject = json.loads(line)
        id_value = lineObject["Url"]
        id_value = id_value[id_value.find('=')+1:]
        if k > start:
            taskQueue.put(id_value)
        k = k + 1
        # if k > start + limit:
        #     break
    print 'load over!'

def CreateDataFromLostData():
    lostfilenames = open(APP_DIR+'lost_app_file','rb')
    k = 1
    limit = 10

    for lostfilename in lostfilenames:
        lostfilename = lostfilename.strip()
        if lostfilename:
            taskQueue.put(lostfilename)
        # k = k + 1
        # if k > limit:
        #     break
    lostfilenames.close()

def TestData():
    test = 'com.checkprice.scanner'
    taskQueue.put(test)

def InitLostAppFile():
    apps=[]
    lostfilenames = open(APP_DIR+'lost_app_file','rb')
    for lostfilename in lostfilenames:
        lostfilename = lostfilename.strip()
        if lostfilename:
            apps.append(lostfilename)
    appset = set(apps)
    lostfilenames.close()
    with open(APP_DIR+'lost_app_file','wb') as f:
        for app in appset:
            append_page(f,app)

if __name__ == '__main__':

    # url = 'https://play.google.com/store/apps/details?id=com.lego.starwars.theyodachronicles'
    #
    # download_url = prefix + url
    #
    # id_value = 'com.lego.starwars.theyodachronicles'
    # googleDownloader = GoogleDownloader()
    # googleDownloader.download(prefix,id_value)
    #
    # # url = 'http://apkleecher.com/apps/2015/03/01/LEGO_STAR_WARS_%201.3_%5Bwww.apkleecher.com%5D.apk'
    # # print "downloading with urllib"
    # # urllib.urlretrieve(url, "code.zip")
    # print "ok"

    # filenames = os.listdir(APP_DIR)
    InitLostAppFile()
    filenames = open(APP_DIR+'finished_app_file','rb')
    for filename in filenames:
        filename = filename.strip()
        if filename:
            APP_MAP[filename] = 1
    filenames.close()
    filenames = open(APP_DIR+'lost_app_file','rb')
    for filename in filenames:
        filename = filename.strip()
        if filename:
            APP_MAP[filename] = 1

    filenames.close()

    CreateDataFromJson()
    # CreateData2()
    # TestData()

    num_threads = 8
    start = time.clock()
    thread_array = []

    for i in range(num_threads):
        google_downloader = GoogleDownloader(str(i))
        google_downloader.start()
        thread_array.append(google_downloader)

    for thread in thread_array:
        thread.join()

    end = time.clock()

    print end - start

    print 'crawler over'