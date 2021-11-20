# uecs2influxdb
UECS(japanese greenhouse IOT Resolution DATA)　to Influxdb.

###### UECS 通信サンプル

```
<?xml version="1.0"?> 
<UECS ver="1.00-E10"> 
<DATA type="SoilTemp.mIC" room="1" region="1" order="1" priority="15">23.0</DATA> 
<IP>192.168.1.64</IP> 
</UECS>
```

###### Influxdb格納サンプル

1. measurement名

   type＝"."(ドット)より左側のみ 小文字であり大文字を使用しない。

   room,region,orderを"_"（アンダースコア）で繋ぎます。

2. measurement：soiltemp_1_1_1

3. Tag

   Cloudは、Cloudのストレージと連携した場合、”1”を付与。それ以外"0"

   DownSampleは、ダウンサンプリングした場合、”1”を付与。それ以外"0"

|          |                            | KEY   | Tag      | TAG   | TAG        |
| -------- | -------------------------- | ----- | -------- | ----- | ---------- |
| カラム名 | datetime                   | VALUE | Priority | Cloud | DownSample |
| 1行      | 2021-11-20 20:19:53.776606 | 23.0  | 15       | 0     | 0          |
|          |                            |       |          |       |            |
|          |                            |       |          |       |            |

