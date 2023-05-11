import requests
import pandas as pd
from datetime import datetime
import os
import certifi
from http.client import RemoteDisconnected    
import logging
import time


def scr_municipality(com):
        url=f"https://nominatim.openstreetmap.org/search.php?q={com}&format=jsonv2"
    #url="https://nominatim.openstreetmap.org/details.php?osmtype=W&osmid=29093098&class=highway&addressdetails=1&hierarchy=0&group_hierarchy=1&format=json"
        try:
            result = requests.get(url=url,verify=certifi.where())
            result_json = result.json()
            tmp=result_json[0]
            s=pd.DataFrame.from_dict(tmp)
            return s.iloc[0][["lat","lon","display_name","category","type"]].to_list() 
        except:
            print(com+" not found..")
            return None

def convert_variable(t,date):
    #print(t)
    lat=t[0]
    long=t[1]
    f=["T2M","T2MDEW","T2MWET","TS","T2M_RANGE","T2M_MAX","T2M_MIN","QV2M","RH2M"]
    x=""
    for i in f:
        x=x+","+str(i)
    x=x[1:]
    quanto="daily"
    from_t=date[0]
    to_t=date[1]
    passs = { "info":t,"quanto": quanto,"f":f,"parameters": x, "long": long, "lat": lat,"from_t": from_t,
             "to_t":  to_t
            }
    return passs

 
def nasa_web_scrap(passs):
    long=float(passs["long"])
    lat=float(passs["lat"])
    quanto=str(passs["quanto"])
    parameters=str(passs["parameters"])
    from_t=str(passs["from_t"])
    to_t=str(passs["to_t"])
    f=passs["f"]
    url=f"https://power.larc.nasa.gov/api/temporal/{quanto}/point?parameters={parameters}&community=RE&longitude={long}&latitude={lat}&start={from_t}&end={to_t}&format=JSON"
    result = requests.get(url=url,verify=certifi.where())
    result_json = result.json()
    param= list(result_json.values())[2]
    L=list(param.values())[0]
    for i in range(len(f)):
        tmp=list(L.values())[i]
        tmp2=pd.DataFrame.from_dict(tmp, orient='index')
        tmp2 = tmp2.rename({0 : f[i]}, axis='columns')
        if i==0:
            df=tmp2
        elif i==1:
            df=pd.merge(df, tmp2, how='left', left_on=df.index, right_on=tmp2.index)
        else: 
            df=pd.merge(df, tmp2, how='left', left_on="key_0", right_on=tmp2.index)
    df['key_0']= pd.to_datetime(df['key_0'])
    tmp2 = tmp2.rename({"key_0" : "period"}, axis='columns')
    return df 


def nasa_forced(passs,maxit):
    x=0
    res=None
    while (res is None) :
        try:
            res=nasa_web_scrap(passs)
        except requests.exceptions.ConnectionError as e:
                    pass
        except Exception as e:
                    logger.error(e)
                    logger.warn('ERROR - Retrying again website %s, retrying in %d secs' % (randomtime))
                    time.sleep(randomtime)
                    continue
        x=x+1
        if x==maxit:
            break
    return res



class MyFilter(object):
    def __init__(self, level):
        self.__level = level

    def filter(self, logRecord):
        return logRecord.levelno <= self.__level
    


#create a logger
logger = logging.getLogger('mylogger')
logger.setLevel(logging.ERROR)
handler = logging.FileHandler('mylog.log')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
#set filter to log only ERROR lines
handler.addFilter(MyFilter(logging.ERROR))
logger.addHandler(handler)

os.chdir("/home/enrico/")


pd_info= pd.read_csv("info_breed.csv",sep=",")

""""


pd_info= pd.read_csv("DATA_ALLEVAMENTI/info_herd.txt",sep=";")

# read dataframe
pd_info= pd.read_csv("DATA_ALLEVAMENTI/info_herd.txt",sep=";")
pd_info.shape
pd_info=pd_info[["CODICE_AIA","COMUNE","FRAZIONE","INDIRIZZO_VIA_NUMERO","CODICE_POSTALE"]]
pd_info.head
pd_info=pd_info.astype({'CODICE_POSTALE':'Int32',"CODICE_AIA":"int"}) # Inr32 missing
pd_info=pd_info.astype("str")


mun=pd_info.iloc[0,1]
date=[20130101,20131210]

t=scr_municipality("LEGNARO,Italy")


def get_climate(mun,date):
    t=scr_municipality(mun)
    passs=convert_variable(t,date)
    res=nasa_web_scrap(passs)
    return res
"""
#pd_info = pd_info[["Comune",""DATAEVENTO_12","DATAEVENTO_11"]]
pd_info=pd_info.drop_duplicates()
final=pd.DataFrame()
collect_error=[]
info_cordinate = []

mun_pos=1
via_pos=2
data_pos_1=4
data_pos_2=5
randomtime = 10 # time to sleep
maxit = 10 
# colonne che non hao usato
cnuove=[0,3]
# SISTEMARE STO LOOP IN BLOCCHO
for i in range(pd_info.shape[0]) :
    tmp=pd_info.iloc[i,mun_pos]
    #tmp1=pd_info.iloc[via_pos,via_pos]
    #tmp=tmp + ","+tmp1
    tmp1="Italy,"+str(tmp)
    #tmp1=str(tmp)  # creo micro dfunzione la rossima volta
    try:
        tmp2=scr_municipality(tmp)
    except requests.exceptions.ConnectionError as e:
            pass
    except Exception as e:
            logger.error(e)
            logger.warn('ERROR - Retrying again website %s, retrying in %d secs' % (randomtime))
            time.sleep(randomtime)
            continue
    if tmp2 is None:
        collect_error.append(tmp) 
    else: 
        datae=pd_info.iloc[i,data_pos_1]
        data1=pd_info.iloc[i,data_pos_2]
        data=[datae,data1]
        passs=convert_variable(tmp2,data)
        res=nasa_forced(passs,maxit)
        if res is None:
            print("not wheter info found ..")
        else :
            res[["lO","LA","MUN","TYPE","IDK"]]=tmp2
        # sonta colonne
            for zonta in cnuove:
                res[[pd_info.columns[zonta]]] = pd_info.iloc[i,].loc[pd_info.columns[zonta]]
            if i==0:
                res.to_csv('REGG_final.csv', mode='a', index=False, header=True)
            else:     
                res.to_csv('REGG_final.csv', mode='a', index=False, header=False)
            del(res)








"""
collect_error=[]
info_cordinate = []
for i in range(pd_info.shape[0]) :
    tmp=pd_info.iloc[i,1].split(",")
    #tmp1="Italy,"+str(tmp)
    tmp2=scr_municipality(tmp)
    if tmp2 is None:
        collect_error.append(tmp) 
        print(tmp +"no ghe ze")
    else:
        info_cordinate.append(tmp2 + [tmp])     
    
pd_info.loc[pd_info["COMUNE"]==collect_error[0],"COMUNE"] = "PRIGNANO"

pf=pd.DataFrame(info_cordinate)
 pf.columns = ["lat","lon","display_name","category","type","COMUNE"]

collect_error=[]
info_cordinate = []
for i in range(pf.shape[0]) :
    
    #tmp1="Italy,"+str(tmp)
    tmp2=scr_municipality(tmp)
    if tmp2 is None:
        collect_error.append(tmp) 
        print(tmp +"no ghe ze")
    else:
        info_cordinate.append(tmp2 + [tmp]) 


collect_error=[]
info_cordinate = []
for i in range(pd_info.shape[0]) :
    tmp=pd_info.iloc[i,1]
    tmp1="Italy,"+str(tmp)
    tmp2=scr_municipality(tmp1)
    if tmp2 is None:
        collect_error.append(tmp1) 
        print(tmp +"no ghe ze")
    else:
        
        info_cordinate.append(tmp2 + [tmp])
"""        
