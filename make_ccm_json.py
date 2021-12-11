from socket import *
import json,os,subprocess
import time as t
import xmltodict
import pandas as pd

def read_ccm_json():
    ccm_json = os.path.dirname(os.path.abspath(__file__)) + '/receive_ccm.json' #CNF
    json_open = open(ccm_json, 'r')
    json_load = json.load(json_open)

    check_list,ccm_list=[],[]
    for k in json_load.keys():
        check_key= json_load[k]["type"] +"_"+ json_load[k]["room"] +"_"+ json_load[k]["region"] +"_"+ json_load[k]["order"]
        check_list.append(check_key)
        ccm_list.append({"json_key": k
                        ,"type":       json_load[k]["type"]
                        ,"room":       json_load[k]["room"]
                        ,"region":     json_load[k]["region"]
                        ,"order":      json_load[k]["order"]
                        ,"sendlevel":  json_load[k]["sendlevel"]
                        ,"savemode":   json_load[k]["savemode"]})


    df_json = pd.DataFrame(ccm_list
        , columns = ['json_key','type','room','region','order','sendlevel','savemode'])


    return set(check_list),df_json


def capture_ccm():
    print('Please wait a few minuite............')
    print('')
    print('sudo systemctl stop uecs2influxdb.service')
    print('')
    cmd = "sudo systemctl stop uecs2influxdb.service"
    subprocess.call( cmd, shell=True )
    print('kill uecs socket............')
    print('')
    cmd = "ps -aux |grep uecs2influxdb|awk \'{print \"sudo kill\",$2}\' | sh"
    subprocess.call( cmd, shell=True )

    #jsonファイルを読み込む
    check_list,df_json = read_ccm_json()

    HOST = ''
    PORT = 16520
    s =socket(AF_INET,SOCK_DGRAM)
    s.bind((HOST, PORT))

    start=t.time()
    end=t.time()
    add_ccm=[]
    while end - start < 50:
        end=t.time()
        msg, address = s.recvfrom(512)

        dictionary = xmltodict.parse(msg)                            # xmlを辞書型へ変換
        json_string = json.dumps(dictionary)                         # json形式のstring
        json_string = json_string.replace('@', '').replace('#', '')  # 「#や@」 をreplace
        json_object = json.loads(json_string)                        # Stringを再度json形式で読み込む

        check_key= (json_object["UECS"]["DATA"]["type"]).lower() \
                    +"_"+ json_object["UECS"]["DATA"]["room"] \
                    +"_"+ json_object["UECS"]["DATA"]["region"] \
                    +"_"+ json_object["UECS"]["DATA"]["order"]

        if check_key not in check_list:
            add_ccm.append({"json_key":  check_key
                    ,"type":       json_object["UECS"]["DATA"]["type"]
                    ,"room":       json_object["UECS"]["DATA"]["room"]
                    ,"region":     json_object["UECS"]["DATA"]["region"]
                    ,"order":      json_object["UECS"]["DATA"]["order"]
                    ,"sendlevel":  ""
                    ,"savemode":   ""
                    })

            check_list.add(check_key)

            print("【" + str(len(add_ccm)) + "件】"
                    , " 残り:"+str(50-round(end - start,1))+"秒 "
                    ,check_key)

    df_ccm = pd.DataFrame(add_ccm
        , columns = ['json_key','type','room','region','order','sendlevel','savemode'])

    df = df_json.append(df_ccm)
    df = df.sort_values(['room','region','order'],ignore_index=True)

    print(df)


#    for l in add_ccm:
#        print(l)
capture_ccm()







