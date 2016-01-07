__author__ = 'vaibhavtyagi'

# Crawler to crawl on wikipedia pages starting from 4 given world war links
# This script crawls using BFS
# Get text content from page and store on disk files
# Which could be later used to index using
import urllib2
from bs4 import BeautifulSoup
from urlparse import urljoin
import urlparse
import time
import re
import elasticsearch


# Create elasticsearch object
es = elasticsearch.Elasticsearch()

# Initialize seed links
url1='http://www.history.com/topics/world-war-ii/battle-of-stalingrad'
url2='http://en.wikipedia.org/wiki/Battle_of_Stalingrad'
url3='http://www.history.com/topics/world-war-ii'
url4='http://en.wikipedia.org/wiki/World_War_II'






fList=list()
fDict=dict()
inDict=dict()
visited=dict()
i=0


#crawl an URL
# Get response from page
# Parse text using beautiful soup python library
def crawlUrl(url):
    print url

    global i
    global fDict
    global fList

    print len(fDict)
    print len(fList)
    print len(visited)
    try:
        response=urllib2.urlopen(url)
        visited[url]=True
        header=response.info()
        type=header.getheader('content-type')
        if 'text/html' in type:
            content = response.read()
            #print content
            soup = BeautifulSoup(content)
            data = soup.encode('utf8').lower()
            if (('world war' in data)|('stalingrad' in data)):
                hList=getLinks(url,soup)
                updateFile(soup,url,hList,header,content)
                i+=1
                print 'i is',i
            else:
                pass

    except:
        print 'Unable to open URL',url
        pass


    #print soup.prettify()


# Get page text into utf8 format to save on disk
# Update inlist for <a> tags on the page
# Index text content on elastic search

def updateFile(soup,url,hList,header,content):
    global inDict

    title=soup.title.get_text().encode('utf8').lower()
    docNo=url
    for script in soup(["script", "style"]):
        script.extract()
    text = soup.get_text()
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text = '\n'.join(chunk for chunk in chunks if chunk)
    text=text.encode('utf8')

    if url in inDict:
        inLinks = inDict[url]
        del inDict[url]
    else:
        inLinks=[]


    out_Links =list(hList)
    #print inLinks
    es.index(index="vs_dataset", doc_type="document",id= url,
              body={"docno": url,
                        "title" : title,
                        "text":text,
                        "in_links":inLinks,
                        "out_links":out_Links,
                        "header":str(header),
                        "raw_html":str(content)})
    '''
    fp=open('contentFile.txt','a')
    fp.write('<DOC>\n')
    fp.write('<DOCNO>'+docNo+'</DOCNO>\n')
    fp.write('<HEAD>'+str(title)+'</HEAD>\n')
    fp.write('<TEXT>\n')
    fp.write(str(text)+'\n')
    fp.write('</TEXT>\n')
    fp.write('</DOC>\n')
    fp.close()
    '''



# Get next url to be crawled and remove previous crawled URL from list
def getNextUrl():
    global fList
    global fDict
    tList = list()
    maxP = max(fDict.values())
    print maxP
    for item in fDict:
        if fDict[item] == maxP:
            tList.append(item)

    minIndex = len(fList)
    itm=''
    url=''
    if minIndex > 0:
        for item in tList:
            tIndex = fList.index(item)
            if minIndex > tIndex:
                itm=item
                minIndex=tIndex


        url = fList[minIndex]
        fList.__delitem__(minIndex)
        del fDict[itm]
    return url


# Clean page to retrive all <a> tags on the page
def getLinks(url,soup):
    global fDict
    global fList
    global visited
    hList = list()
    aList= soup.find_all('a')
    #print soup.get_text()
    for link in aList:
        href=link.get('href') # get the link
        href=urljoin(url,href) # join for absolute path
        href=getCanonical(href)
        try:
            href = href.encode('utf8')
        except:
            continue

        if href == url:
            continue
        hList.append(href)
        try:
            if visited[href]==True:
                continue
            else:
               fDict[href]=fDict[href]+1
        except:
            fDict[href]=1
            fList.append(href)
            visited[href]=False
    updateInlink(url,set(hList));
    #generateGraph(url,set(hList))
    return set(hList)


# Update inlink list for a url, whenever a url is seen as <a> tag on a page crawled
def updateInlink(url,hList):
    global inDict;
    for link in hList:
        try:
            es_body = {u'in_links': {"script": 'updateInLinks', "params": {"new_link": url}}}
            res = es.update(index='vs_dataset', doc_type='document',
	                id=link, body=es_body)
        except:
            try:
                inDict[link].append[url]
            except:
                inDict[link]=[url]


# Get canonical form of a URL
def getCanonical(href):
    href=urlparse.urlsplit(href) #split url into components
    scheme = href[0].lower()
    netloc = href[1].lower()
    # code for removing port
    try:
	    lPort = re.findall(r':[0-9]+',netloc)
	    netloc = netloc.replace(lPort[0],"")
    except:
	    pass
    # remove // from the path
    path=href[2].replace("//","/")
    query=""
    fragment=""
    href=(scheme,netloc,path,query,fragment)
    # join url components
    href=urlparse.urlunsplit(href)
    return href


# Write url and outlinks mapping on disk
def generateGraph(url,hList):
    fp=open('OutLinkFile.txt','a')
    fp.write(url)
    for href in hList:
        fp.write('  '+href.encode('utf8'))
    fp.write('\n')


# Write inlinks and url mapping output files on disk
def writeInLink(inDict):
    fp=open("InLinkFile.txt",'a')
    for key in inDict:
        fp.write(key+'  ')
        for link in inDict[key]:
            fp.write(link+' ')
        fp.write('\n')
    fp.close()

url1=getCanonical(url1.encode('utf8'))
url2=getCanonical(url2.encode('utf8'))
url3=getCanonical(url3.encode('utf8'))
url4=getCanonical(url4.encode('utf8'))



crawlUrl(url1)
crawlUrl(url2)
crawlUrl(url3)
crawlUrl(url4)

# Get next URL to crawl using BFS search
# Check if the url is already visited or not
# Crawl on this url and update url to next url
url=getNextUrl()
while url!='':
    print i
    if i==15000:
        break
    if visited[url] == True:
        url=getNextUrl()
        continue
    else:
        crawlUrl(url)
        time.sleep(1)
        url=getNextUrl()
        i+=1



#writeInLink(inDict)