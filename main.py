# -*- coding: utf-8 -*-
"""
Created on Sun Dec 10 14:02:16 2017

@author: SML
"""

import math
import myFunction
from pymongo import MongoClient

    

# MongoDB에 데이터를 삽입시키는 함
def insertDataToMongoDB(startDay, date, average, stdev, lCount, hCount, db):
    
    busStationInfo = dict() #정류장 ID를 Key로 갖고, 승하차인원을 Value로 갖는 딕셔너리.(기간내에 승하차 인원을 누적된값으로 갖는다)
    lowStationInfo = dict() # busStationInfo중 value가 10 이하인 딕셔너리.
    highStationInfo = dict() # busStationInfo중 value가 상위 5%이상인 정류.
    dayCount = dict()   #정류장 ID를 Key로 갖고, 기간내에 API에 기록된 버스정류장의 Count를 Value로 갖는다.
    
    totalBusStationInfo = 0 #busStationInfo의 Value의 총합, 평균값을 구하기 위함이다. 
    diffSum = 0 # 분산을 구하기 위한 변수 .
    upperBound = 0.0 # 상위 5%이상인 승하차인원의 값을 담는 변수 .
#    lowerBound = 0.0
    
    
    myFunction.getInfo(date, startDay, busStationInfo, dayCount) #주중 혹은 주말기간 동안만큼 count하여 getInfoFunction을 호출.
    
    for key, value in busStationInfo.iteritems():
        busStationInfo[key] = float(value) / float(dayCount[key])   #busStationInfo딕셔너리의 값을 기간내의 평균 승하차인원으로 다시 값을 담아준다. 
    
    count = 0
    for key, value in busStationInfo.iteritems(): #평균을 구한다.
        totalBusStationInfo = totalBusStationInfo + value
        count = count + 1
    average = float(totalBusStationInfo)/float(count)
    

# 상위 95% Z = 1.65, 하위 25% Z = -0.68   
    for key, value in busStationInfo.iteritems():
        diffSum = diffSum + (average - value)**2
    stdev = math.sqrt(diffSum/count) #stdev는 표준편차이다.
    upperBound = stdev*1.65 + average # 상위 95% 이상. Z값을 이용하여 상위 95%인 버스정류소 승하차인원을 구한다. 

#하위 25% 이하
#    lowerBound = -(stdev*0.68) + average

#   상위 약95% 이상 :  577.7, 536개
#   하위 약25% 이하 :  75.12
#    print "upperBound : ", upperBound
#    print "lowerBound : ", lowerBound

    for key, value in busStationInfo.iteritems(): #승하차인원이 10명 이하인 버스정류소의 ID와 승하차인원.
        if(value <= 10):                          #그리고 승하차인원이 상위 5%이상인 버스정류소의 ID와 승하차 인원을 딕셔너리에 담는다.
            lowStationInfo[key] = value
            lCount = lCount + 1
        if(value >= upperBound):    #upperBound보다 값이 크다면 highStationInfo에 담는다.
            highStationInfo[key] = value
            hCount = hCount + 1

    myFunction.getInfoHigh(date, startDay, highStationInfo, db.highBusData)    #몽고DB에 highBusData 컬렉션과 lowBusData컬렉션에
    myFunction.getInfoLow(date, startDay, lowStationInfo, db.lowBusData)      #정류소 이름, ID, 승하차인원을 딕셔너리를 참조하여 담아준다.


# 몽고db에서 데이터를 읽어오는 함수 
def searchDataFromMongoDB(db):
    highResults = db.highBusData.find() #몽고db의 highBusData 컬렉션을 찾아 highResults에 담는다. 
    lowResults = db.lowBusData.find()   #몽고db의 lowBusData 컬렉션을 찾아 lowResults에 담는다. 
    totalNum =0 #각 컬렉션의 데이터 갯수를 파악하기 위한 변수

    print "***************************************************************************************"
    print u"****************승하차 이용 고객수가 상위 5%이상인 버스 정류장**************************"
    print "***************************************************************************************"
    
    #데이터를 출력해준다.
    for r in highResults:
        print totalNum+1, '. ', u"정류소 명 : ", r["BUS_STA_NM"], u"\t\t정류소 ID : ", r["BSST_ARS_NO"], u"\t\t승하차 인원 : ", r["TOT_PASGR_NO"]
        totalNum += 1
    print u"전체 정류장 수 : ", totalNum   
    
    totalNum = 0
    print '\n\n******************************************************************************************'
    print u'*********************승하차 이용 고객수가 10명 이하인 버스 정류장****************************'
    print '*******************************************************************************************'
    
    #데이터를 출력해준다.
    for r in lowResults:
        print totalNum+1, '. ', u"정류소 명 : ", r["BUS_STA_NM"], u"\t\t정류소 ID : ", r["BSST_ARS_NO"], u"\t\t승하차 인원 : ", r["TOT_PASGR_NO"]
        totalNum += 1
    print u"전체 정류장 수 : ", totalNum   

#main함수 호출 이전에 실행되는 함수로 컬렉션과 데이터가 없다면 호출하여 api를통해 데이터를 몽고db에 삽입한다.
#즉, 데이터를 insert하기 위한 용도의 함수이다.
def prevMain():
    average = 0.0
    stdev = 0.0
    lCount = 0
    hCount = 0
    startDay = 'monday' #시작일이 monday이면 주중의 데이터를 구한다. saturday이면 주말의 데이터를 구한다.
    date = 20171106 #시작일자
    client = MongoClient() #몽고db와 연결
    db = client.busDB #몽고db의 busDB 데이터베이스를 변수에 저장
    insertDataToMongoDB(startDay, date, average, stdev, lCount, hCount, db) #insertDataToMongoDB함수 호출.

def main():
    client = MongoClient() #몽고db와 연결.
    db = client.busDB #몽고db의 busDB 데이터베이스를 변수에 저장.
    searchDataFromMongoDB(db) #몽고db로부터 Data를 불러온다.

if __name__ == "__main__":
    
    prevMain()   #데이터를 몽고DB에 저장하는 함수로 실행을 통해 데이터를 
                #몽고 DB에 저장하였다면 함수를 주석처리해 더이상 실행되지 않도록 한다.
    main()