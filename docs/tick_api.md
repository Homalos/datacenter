Request URL: https://hq.sinajs.cn/?list=nf_SA2601

Request Method: GET

参数：list=nf_{symbol}，symbol代表6位合约代码

举例：https://hq.sinajs.cn/?list=nf_SA2601

HTTP/1.1 

Request Header：

Accept: \*/\* 

Accept-Encoding: gzip, deflate, br, zstd 

Accept-Language: zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7 

Connection: keep-alive 

Host: hq.sinajs.cn 

Referer: https://finance.sina.com.cn

Sec-Fetch-Dest: script 

Sec-Fetch-Mode: no-cors 

Sec-Fetch-Site: cross-site 

Sec-Fetch-Storage-Access: active 

User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 

sec-ch-ua: "Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141" 

sec-ch-ua-mobile: ?0 

sec-ch-ua-platform: "Windows"



Response Header：

Cache-Control: no-cache 

Content-Length: 130 

Connection: Keep-Alive 

Content-Type: application/javascript; charset=GB18030 

Content-Encoding: gzip



Response(SA2601举例):

var hq_str_nf_SA2601 = "纯碱2601,150000,1233.000,1238.000,1222.000,1232.000,1231.000,1232.000,1232.000,1231.000,1229.000,973,279,1377333.000,794201,郑,纯碱,2025-10-15,1,,,,,,,,,1231.000,0.000,0,0.000,0,0.000,0,0.000,0,0.000,0,0.000,0,0.000,0,0.000,0";

返回关键按顺序解释：

纯碱2601：合约中文代码(英文代码是SA2601)

150000：不明数字（可能是数据提供商代码）

1233.000：开盘价

1238.000：最高价

1222.000：最低价

1232.000：昨日收盘价

1231.000：买价，即“买一”报价 

1232.000：卖价，即“卖一”报价

1232.000：最新价，即收盘价

1231.000：结算价

1229.000：昨结算

973：买量

279：卖量

1377333.000：持仓量

794201：成交量

郑：郑州商品交易所简称

纯碱：品种名简称

2025-10-15：日期，即交易日

后面的数据不做关注