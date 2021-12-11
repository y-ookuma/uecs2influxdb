#----------------------------------------------------------------------*/
# 2021.11.19 UECS 
# sudo apt-get install python3-pip -y
# sudo apt-get install python3-pandas -y
# sudo apt-get install python3-influxdb -y
# sudo pip3 install xmltodict
#----------------------------------------------------------------------*/
from socket import *
import time as t
import datetime as dt
import pandas as pd
import xmltodict,json,os,configparser
from multiprocessing import Process 
from influxdb import InfluxDBClient

## 初期設定
class Initialset():
    def parm_set():
        # 受信設定ファイルを読み込む。
        ccm_json = os.path.dirname(os.path.abspath(__file__)) + '/receive_ccm.json' #CNF
        json_open = open(ccm_json, 'r')
        json_load = json.load(json_open)
        # 保存用種別を4つ作成
        up_ ,diff_ ,max_ ,abc_=[],[],[],[]

        for k in json_load.keys():
            recv_ccm = json_load[k]["type"].split(".")[0]
            recv_ccm += "_" + json_load[k]["room"]
            recv_ccm += "_" + json_load[k]["region"]
            recv_ccm += "_" + json_load[k]["order"]
            recv_ccm = recv_ccm.lower()                   # 小文字に変換
            if json_load[k]["savemode"] not in (None,""):
                up_.append(recv_ccm)
            if json_load[k]["savemode"] == "diff":
                diff_.append(recv_ccm)
            elif json_load[k]["savemode"] in ("on","off"):
                max_.append(recv_ccm)
            elif json_load[k]["savemode"] =="abc":
                abc_.append(recv_ccm)

        recv_ccm = {"flag_up":set(up_) ,"flag_diff":set(diff_) ,  # 集合型に変換
                    "flag_max":set(max_) ,"flag_abc":set(abc_)}

        # read config
        filepath = os.path.dirname(os.path.abspath(__file__))+ '/uecs2influxdb.cfg'
        config = configparser.ConfigParser()
        config.read(filepath)

        return recv_ccm,config

## UDP受信クラス
class udprecv():
    def __init__(self,config):

        SrcIP = ""                                               # 受信元IP
        SrcPort = 16520                                          # 受信元ポート番号
        self.SrcAddr = (SrcIP, SrcPort)                          # アドレスをtupleに格納

        self.BUFSIZE = 512                                       # バッファサイズ指定
        self.udpServSock = socket(AF_INET, SOCK_DGRAM)           # ソケット作成
        self.udpServSock.bind(self.SrcAddr)                      # 受信元アドレスでバインド
#        manager = Manager()                                      # 共有メモリ
#        self.queue=manager.list()                                # 空のリストを定義
        # influxdb パラメータ
        self.client = InfluxDBClient(config["influxdb"]["host_name"]        #local DB InfluxDBClient
                                    ,int(config["influxdb"]["port"])
                                    ,config["influxdb"]["user"]
                                    ,config["influxdb"]["pass"]
                                    ,config["influxdb"]["database"]
                                    , timeout=3, retries=3 )

        self.cloud = InfluxDBClient(config["influxdb_cloud"]["host_name"]   #cloud DB
                                    ,int(config["influxdb_cloud"]["port"])
                                    ,config["influxdb_cloud"]["user"]
                                    ,config["influxdb_cloud"]["pass"]
                                    ,config["influxdb_cloud"]["database"]
                                    , timeout=3, retries=3 )


    def recv(self,debug=False,debug_sec=None,ccm_list=[]):
        if debug_sec is not None:    # debug_sec が指定されている場合
            start=t.time()
            debug_list=[]
            print("ccm_list",ccm_list)

        while True:                                              # 常に受信待ち
            ccm, addr = self.udpServSock.recvfrom(self.BUFSIZE)  # 受信
#            print(ccm.decode(), addr)                            # 受信データと送信アドレス表示

            p = Process(target=self.save_df, args=(debug,ccm,ccm_list))        # マルチプロセス化でDB処理などを実行する
            p.start()
            if p.join(5) is None:
                p.terminate()
            # デバックモード 
            if debug: 
                print(ccm.decode(), addr)                           # 受信データと送信アドレス表示
                if debug_sec is not None:                           # 秒 指定がある場合
                    end=t.time()
                    debug_list.append(ccm)
                    print("Main process ID:",os.getppid())
                    if end-start>=debug_sec:
                        print("debug_time:",round(end-start,2),"ExecCount:",len(debug_list))
                        break;

    # DB保存処理
    def save_df(self,debug,ccm,ccm_list):
        dictionary = xmltodict.parse(ccm)                            # xmlを辞書型へ変換
        json_string = json.dumps(dictionary)                         # json形式のstring
        json_string = json_string.replace('@', '').replace('#', '')  # 「#や@」 をreplace
        json_object = json.loads(json_string)                        # Stringを再度json形式で読み込む

        measurement = json_object["UECS"]["DATA"]["type"].split(".")[0]
        measurement += "_" + json_object["UECS"]["DATA"]["room"]
        measurement += "_" + json_object["UECS"]["DATA"]["region"]
        measurement += "_" + json_object["UECS"]["DATA"]["order"]
        measurement = measurement.lower()                             # 小文字に変換
        datetime    = pd.to_datetime(dt.datetime.now())
        val         = float(json_object["UECS"]["DATA"]["text"])*1.0
        priority    = json_object["UECS"]["DATA"]["priority"]

        # 保存用のCCMでない場合、EXITする
        if measurement not in ccm_list["flag_up"]:
            exit(0)

        # 前回のデータと今回のデータの差分処理
        if measurement in ccm_list["flag_diff"] :
            script = 'select last(*) from %s where cloud=\'0\' and downsample=\'0\';' % measurement 
            rs = self.influx_query(debug,script)         
            rs = list(rs.get_points(measurement=measurement))
            last_val = float(rs[0]["last_value"])
            if last_val is None: #前回のデータがなければ、今回のデータを使う
                last_val= val
            val = abs(val - last_val) #絶対値

        # 四捨五入する 0,1 のデータを対象とする
        if measurement in ccm_list["flag_max"]:
            val = round(val) * 1.0

        # ↓↓↓  influxDB用整形
        json_body = [{"measurement": measurement,
                      "tags": {"priority": priority,"cloud": "0","downsample": "0"},
                      "time": datetime,
                      "precision": "s",
                      "fields": {"value": val}
                    }]
        # ↑↑↑  influxDB用整形
        p = Process(target=self.influx_write, args=(debug,json_body))        # マルチプロセス化でDB処理などを実行する
        p.start()
        if p.join(5) is None:
            p.terminate()

        if debug:
            print("Sub_process ID:",os.getppid())

    def influx_write(self,debug,json_body):
        try:
            self.client.write_points(json_body)
            if debug:
                print(json_body)
        except :
            print(json_body)
            print("influxdb insert処理に失敗しました。")
            pass

    def influx_query(self,debug,script):
        try:
            rs = self.client.query(script)
        except :
            print(script)
            print("influxdb query処理に失敗しました。")
            pass
        return rs


#-------------------------------------------------------#
# Main 処理
#-------------------------------------------------------#

# パラメータセット
ccm_list,config=Initialset.parm_set()

#UECS受信
udp = udprecv(config)     # クラス呼び出し
#udp.recv(debug=True,debug_sec=10,ccm_list=ccm_list)     # デバックモード　10秒間
udp.recv(debug=True,ccm_list=ccm_list)                 # 本番処理　debug_secを指定しない

