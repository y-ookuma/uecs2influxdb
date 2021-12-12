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
    if json_load is not None:
        for k in json_load.keys():
            check_key= json_load[k]["type"].lower() +"_"+ json_load[k]["room"] +"_"+ json_load[k]["region"] +"_"+ json_load[k]["order"]
            check_list.append(check_key)
            ccm_list.append({"json_key": k
                        ,"type":       json_load[k]["type"].lower()
                        ,"room":       json_load[k]["room"]
                        ,"region":     json_load[k]["region"]
                        ,"order":      json_load[k]["order"]
                        ,"sendlevel":  json_load[k]["sendlevel"]
                        ,"savemode":   json_load[k]["savemode"]})

        df_json = pd.DataFrame(ccm_list
            , columns = ['json_key','type','room','region','order','sendlevel','savemode'])
#    print(check_list)
        return set(check_list),df_json
    else:
        return None,None


def kill_uecs_proc():
    print('-------------------------------------')
    print(' receive_ccm.json 作成処理')
    print('-------------------------------------')
    print('')
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

def start_uecs_proc():
    print('sudo systemctl start uecs2influxdb.service')
    print('')
    cmd = "sudo systemctl start uecs2influxdb.service"
    subprocess.call( cmd, shell=True )
    print('------------------------------------------------------')
    print(' 正常に uecs2influxdb が動作していることを確認ください')
    print('------------------------------------------------------')


def capture_ccm():
    print('-------------------------------------')
    print(' 以下のCCMを取り込んでいます.........')
    print(' 50秒間かかります...................')
    print('-------------------------------------')

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
                    ,"type":       json_object["UECS"]["DATA"]["type"].lower()
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

    # receive_ccm.json と CCMキャプチャとの結合
    df = df_json.append(df_ccm)
    print(df)
    df.sort_values(['room','region','order','type'],ignore_index=True)  # ソート
    df.drop_duplicates(subset=['room','region','order','type'], keep='last')  #重複行を削除
    print(df)
    df.set_index("json_key",inplace=True) #インデックス を "json_key"に変更

    print(df)
    if df is not None:
        output_json = df.to_json(orient="index",force_ascii=False)   #形式を指定: 全角文字（日本語）などのUnicodeエスケープ指定

        output_json = json.loads(output_json)
        path = os.path.dirname(os.path.abspath(__file__)) + '/receive_ccm.json' #CCMのデータをreceive_ccm.jsonに保存する
        with open(path, 'w') as f:
            json.dump(output_json, f, ensure_ascii=False, indent=4)  #整形して出力

        print('-------------------------------------------------')
        print(' receive_ccm.json を再作成完了しました。.........')
        print('-------------------------------------------------')
    else:
        print('-------------------------------------------------')
        print(' receive_ccm.json の変更はありません。.........')
        print('-------------------------------------------------')



kill_uecs_proc()
capture_ccm()
start_uecs_proc()
