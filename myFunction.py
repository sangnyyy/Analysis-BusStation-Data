# -*- coding: utf-8 -*-
"""
Created on Sun Dec 10 14:09:28 2017

@author: SML
"""
import urlparse
import requests

KEY = '6976745741736d613131334d79476162'  #API를 이용하기위한 인증.
TYPE = 'json' #타입.
SERVICE='CardBusStatisticsServiceNew' #서비스.
_url = 'http://openapi.seoul.go.kr:8088/' #주소.
totalBusInfo = list()   #API를 통하여 가져오게되는 데이터를 담게되는 리스트.

#API에서 제공하는 특정일자의 데이터 갯수를 담는 함.
def searchListTotalCount(date):
    USE_DT = str(date) 
    param = KEY+'/'+TYPE+'/'+SERVICE+'/'+'1'+'/'+'1'+'/'+USE_DT
    url = urlparse.urljoin(_url, param)
    r = requests.get(url)
    temp = r.json()
    return temp['CardBusStatisticsServiceNew']['list_total_count'] #list_total_count가 데이터 전체 갯수를 뜻한다.

#주중 혹은 주말기간 동안만큼 count하여 getInfoFunction을 호출.
def getInfo(date, startDay, busStationInfo, dayCount):
    tempDate = date #임시로 파라미터로 전달된 date를 저장.
    if(startDay == 'monday'): #파라미터 startDay가 'monday'면 주중에 데이터를 가져온다. 즉, 5일치 데이터를 읽어와야 한다.
        endDate = date + 5 #endDate를 date+5.
        
             
        #에러가 발생하지 않았다면 적합한 date임으로 date가 endDate보다 
        #작은동안 getInfoFunction호출.
        while(date < endDate):
            getInfoFunction(date, busStationInfo, dayCount) #파라미터로 date, busStationInfo, dayCount를 넘긴다.
            date = date + 1
        date = tempDate  #조작되어진 date를 원래 date로 돌려준다.
        
    elif(startDay == 'saturday'): #파라미터 startDay가 'saturday'면 주말에 데이터를 가져온다. 즉, 2일치 데이터를 읽어와야 한다.
        endDate = date + 2      #"endDate = date + 2"를 제외한 위의 if문과 같은방식으로 진행된다.
        

        while(date < endDate):
            getInfoFunction(date, busStationInfo, dayCount)
            date = date + 1
        date = tempDate
        
    else :  #else문의 경우 startDay가 적합하지 않으므로 에러처리를한
        print "invalid startDay.."
        return 

#insertDataToMongoDB함수 안에서 정의된 highStationInfo딕셔너리를 주어진 기간에 대해서
#몽고db에 삽입시키기 위한 함수이다.
def getInfoHigh(date, startDay, StationInfo, highBusData): #파라미터로 date, startDay, StationInfo, highBusData를 전달 받는다.  
    tempDate = date  #date를 임시로 저장.
    if(startDay == 'monday'):   #startDay가 'monday'라면 진행.
        endDate = date + 5  
        while(date < endDate):  #주중 5일 동안에 진행된다.
            getInfoFunctionHigh(date, StationInfo, highBusData) #getInfoFunctionHigh(date, StationInfo, highBusData)를 호출.
            date = date + 1 #date를 1만큼 증가
        date = tempDate  #조작된 date를 원래의 date로 복원.
    
    elif(startDay == 'saturday'):  #startDay가 'saturday'라면 진행.
        endDate = date + 2
        while(date < endDate):  #주말 2일 동안에 진행된다.
            getInfoFunctionHigh(date, StationInfo, highBusData) #getInfoFunctionHigh(date, StationInfo, highBusData)를 호출.
            date = date + 1 #date를 1만큼 증가
        date = tempDate #조작된 date를 원래의 date로 복원.
    else :  #startDay가 잘못된 경우에 에러를 출력하고 종료. 
        print "invalid startDay"
        return 
    
    
#insertDataToMongoDB함수 안에서 정의된 lowStationInfo딕셔너리를 주어진 기간에 대해서
#몽고db에 삽입시키기 위한 함수이다. getInfoHigh와 진행방식은 동일하다.  
def getInfoLow(date, startDay, StationInfo, lowBusData):
    tempDate = date
    if(startDay == 'monday'):
        endDate = date + 5
        while(date < endDate):
            getInfoFunctionLow(date, StationInfo, lowBusData)
            date = date + 1
        date = tempDate
    
    elif(startDay == 'saturday'):
        endDate = date + 2
        while(date < endDate):
            getInfoFunctionLow(date, StationInfo, lowBusData)
            date = date + 1
        date = tempDate
    else :
        print "invalid startDay"
        return 

#getInfo함수가 내부에서 호출한 함수이다. date, busStationInfo, dayCount를 파라미터로 전달받으며
#API를 통하여 데이터를 읽어오는 작업을 하는 함수이다.
def getInfoFunction(date, busStationInfo, dayCount):
        list_total_count = searchListTotalCount(date)   #searchListTotalCount함수를 통하여 전체 데이터 갯수를 읽어온다.
        page = (list_total_count / 1000)  + 1   #1000개 단위의 페이지 갯수를 담는다
        remain = list_total_count % 1000    #1000개단위의 페이지에 마지막으로 남은 갯수를 담는다.

        tempPage = 1    #임시 페이지
        tempStart = 1   #임시 API 데이터 시작위치
        tempLast = 1000 #임시 API 데이터 종료위치

        #API를 통하여 데이터를 읽어서 totalBusInfo리스트에 저장하는 작업이다.
        while(tempPage <= page):    #페이지와 작거나 같은동안 진행.
            print "Processing getInfoFunction of getInfo..." #while문이 진행되는것을 명시하기 위한 print문.
            if(tempPage != page):   #tempPage가 page와 같지않다면 if문 아래를 진행.
                START_INDEX=str(tempStart)  #시작위치를 string형태로 변환하여 저장.
                END_INDEX=str(tempLast) #종료위치를 string형태로 변환하여 저장.
                USE_DT = str(date)  #API를 통하여 가져오게되는 데이터의 날짜.
                param = KEY+'/'+TYPE+'/'+SERVICE+'/'+START_INDEX+'/'+END_INDEX+'/'+USE_DT #URL에 전달하게될 파라미터를 저장.
                url=urlparse.urljoin(_url,param)    #urlparse모듈의 urljoin내장함수를 통하여 url과 param을 파싱하여 저장.
                r = requests.get(url)   #get메서드를 통하여 url의 데이터를 크롤링해온다. 
                totalBusInfo.append(r.json())   #크롤링하여 저장한 데이터를 json형태로 변환하여 totalBusInfo에 추가한다.
                tempPage = tempPage + 1 #페이지를 1만큼 증가시킨다.
                tempStart = tempStart + 1000 #시작위치를 1000만큼 증가.
                tempLast = tempLast + 1000  #종료위치 또한 1000만큼 증가.
            else: #마지막 페이지의 API 데이터를 가져오는 부분이다. 위의 if문과 같은 방식으로 진행된다.
                START_INDEX=str(tempStart)  
                END_INDEX=str(tempStart + remain - 1)   
                USE_DT = str(date)  
                param = KEY+'/'+TYPE+'/'+SERVICE+'/'+START_INDEX+'/'+END_INDEX+'/'+USE_DT
                url=urlparse.urljoin(_url,param)
                r = requests.get(url)
                totalBusInfo.append(r.json())
                tempPage = tempPage + 1 #tempPage가 1만큼 증가하여 page보다 커지므로 while문을 탈출하게 된다.
                
        tempPage = 1    #다음 진행을 위해 원래의 값으로 돌려준다.
        tempStart = 1   #다음 진행을 위해 원래의 값으로 돌려준다
        tempLast =1000  #다음 진행을 위해 원래의 값으로 돌려준다
                
        
        
        #위의 while문에서 list에 담은 data들을 busStationInfo, dayCount 딕셔너리에 저장하는 과정이다.
        #busStationInfo는 key로 버스의 고유한 ARS Number를 가지며, value로 주어진 기간동안에 해당 버스정류장을 이용한 
        #승하차 고객의 누적값을 갖는다. dayCount는 key로 버스의 고유한 ARS Number를 가지며 value로 주어진 기간동안에 API의 모든데이터의 
        #해당 정류장에 대한 데이터의 갯수를 담는다.(말이 좀 어렵네요...)
        #즉, 이후에 dayCount의 value로 busStationInfo를 나눠서 주어진 기간동안 버스정류장을 이용한 승하차 고객의 평균값을 구하고자 하기 위함이다.
        while(tempPage <= page):     #페이지와 작거나 같은동안 진행.
            print "Processing getInfoFunction of getInfo..."  #while문이 진행되는것을 명시하기 위한 print문.
            if(tempPage != page):    #tempPage가 page와 같지않다면 if문 아래를 진행.
                for e in totalBusInfo[tempPage - 1]['CardBusStatisticsServiceNew']['row']:  #totalBusInfo의 리스트의 원소의 각각에 데이터에 대하여 진행한다.
                    if str(e['BSST_ARS_NO']) not in busStationInfo: #만약 데이터의 버스의 ARS No가 busStationInfo안에 있다면
                        busStationInfo[str(e['BSST_ARS_NO'])] = float(e['RIDE_PASGR_NUM'])+ float(e['ALIGHT_PASGR_NUM']) #busStationInfo의 value에 승하차인원을 담아준다.
                        dayCount[str(e['BSST_ARS_NO'])] = 1 #dayCount의 value를 1로 저장해준다.
                    else:   #만약 데이터의 버스의 ARS No가 busStationInfo안에 없다면
                        busStationInfo[str(e['BSST_ARS_NO'])] = busStationInfo[str(e['BSST_ARS_NO'])] + float(e['RIDE_PASGR_NUM'])+ float(e['ALIGHT_PASGR_NUM'])    #busStationInfo의 value에 승하차인원을 더해준다.
                        dayCount[str(e['BSST_ARS_NO'])] = dayCount[str(e['BSST_ARS_NO'])] + 1   #dayCount의 value를 1만큼 증가시킨다.
                tempPage = tempPage + 1 #tempPage를 1만큼 증가시킨다.
            else :  #마지막 페이지에 대하여 진행한다.
                for e in totalBusInfo[tempPage -1]['CardBusStatisticsServiceNew']['row']:   #위의 if문과 동일하게 진행한다.
                    if str(e['BSST_ARS_NO']) not in busStationInfo:
                        busStationInfo[str(e['BSST_ARS_NO'])] = float(e['RIDE_PASGR_NUM'])+ float(e['ALIGHT_PASGR_NUM'])
                        dayCount[str(e['BSST_ARS_NO'])] = 1
                    else:
                        busStationInfo[str(e['BSST_ARS_NO'])] = busStationInfo[str(e['BSST_ARS_NO'])] + float(e['RIDE_PASGR_NUM'])+ float(e['ALIGHT_PASGR_NUM'])
                        dayCount[str(e['BSST_ARS_NO'])] = dayCount[str(e['BSST_ARS_NO'])] + 1

                tempPage = tempPage + 1 #tempPage가 page보다 커졌으므로 while문을 탈출하며 getInfoFunction함수가 종료된다.
          
#getInfoHigh함수의 내부에서 호출된 함수로 파라미터로 date, highStationInfo, highBusData를 전달받는다.
#실질적으로 몽고 db의 highBusData 컬렉션에 데이터를 저장하는 과정이 수행된다.
def getInfoFunctionHigh(date, StationInfo, highBusData):
    list_total_count = searchListTotalCount(date)   #API의 전체 데이터의 갯수.
    page = (list_total_count / 1000)  + 1   #1000개 단위의 데이터를 페이지로 나누어 구한다.

    tempPage = 1    #임시 페이지를 1로 저장한다.
    

        
    while(tempPage <= page):    #tempPage <=page인 동안에 진행.
        print "Processing getInfoFunctionHigh of getInfoHigh..."  #해당 함수가 진행되고 있음을 명시.
        Check = dict()  #임시로 딕셔너리 생
        if(tempPage != page):   #tempPage가 page가 아니면 진행.
            for e in totalBusInfo[tempPage - 1]['CardBusStatisticsServiceNew']['row']:  #totalBusInfo의 리스트의 원소의 각각에 데이터에 대하여 진행한다.
                Check = StationInfo  #Check딕셔너리에 StationInfo의 값을 저장한다.
                if str(e['BSST_ARS_NO']) in Check and Check[str(e['BSST_ARS_NO'])] != 0:    #Check안에 버스정류장 ARS No가 있고 그 Value가 0이 아니면 진행. 
                    #highBusData 컬렉션에 버스정류장 이름, 버스정류장 ARS No, 승하차 인원을 저장한다.
                    highBusData.insert_one({"BUS_STA_NM" : e['BUS_STA_NM'], "BSST_ARS_NO" : e['BSST_ARS_NO'], "TOT_PASGR_NO" : int(StationInfo[e['BSST_ARS_NO']])}) 
                    Check[str(e['BSST_ARS_NO'])] = 0    #이후에 if문 이하를 실행하지 못하게 하기위하여(중복을 막기위해) 버스 ARS No에 대한 value를 0으로 바꿔준다.   
            tempPage = tempPage + 1 #tempPage를 1만큼 증가.


        else :  #마지막 페이지에 대하여 진행, 위의 if문과 같은 방식으로 진행된다.
            for e in totalBusInfo[tempPage -1]['CardBusStatisticsServiceNew']['row']:
                Check = StationInfo    
                if str(e['BSST_ARS_NO']) in Check and Check[str(e['BSST_ARS_NO'])] != 0:
                    highBusData.insert_one({"BUS_STA_NM" : e['BUS_STA_NM'], "BSST_ARS_NO" : e['BSST_ARS_NO'], "TOT_PASGR_NO" : int(StationInfo[e['BSST_ARS_NO']])})
                    Check[str(e['BSST_ARS_NO'])] = 0        
            tempPage = tempPage + 1

#getInfoLow함수의 내부에서 호출된 함수로 파라미터로 date, lowStationInfo, lowBusData를 전달받는다.
#실질적으로 몽고 db의 lowBusData 컬렉션에 데이터를 저장하는 과정이 수행된다.
#위의 getInfoFunctionHigh함수와 같은방식으로 진행된다.
def getInfoFunctionLow(date, StationInfo, lowBusData):
    list_total_count = searchListTotalCount(date)
    page = (list_total_count / 1000)  + 1

    tempPage = 1
    
    while(tempPage <= page):
        print "Processing getInfoFunction3 of getInfo3..."
        Check = dict()
        if(tempPage != page):
            for e in totalBusInfo[tempPage - 1]['CardBusStatisticsServiceNew']['row']:
                Check = StationInfo    
                if str(e['BSST_ARS_NO']) in Check and Check[str(e['BSST_ARS_NO'])] != 0:
                    lowBusData.insert_one({"BUS_STA_NM" : e['BUS_STA_NM'], "BSST_ARS_NO" : e['BSST_ARS_NO'], "TOT_PASGR_NO" : int(StationInfo[e['BSST_ARS_NO']])})
                    Check[str(e['BSST_ARS_NO'])] = 0   
            tempPage = tempPage + 1


        else :
            for e in totalBusInfo[tempPage -1]['CardBusStatisticsServiceNew']['row']:
                Check = StationInfo    
                if str(e['BSST_ARS_NO']) in Check and Check[str(e['BSST_ARS_NO'])] != 0:
                    lowBusData.insert_one({"BUS_STA_NM" : e['BUS_STA_NM'], "BSST_ARS_NO" : e['BSST_ARS_NO'], "TOT_PASGR_NO" : int(StationInfo[e['BSST_ARS_NO']])})
                    Check[str(e['BSST_ARS_NO'])] = 0        
            tempPage = tempPage + 1