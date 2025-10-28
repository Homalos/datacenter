```bash
cd web-ui && npm install vue-router@4 pinia element-plus @element-plus/icons-vue axios
```

```bash
# 1. 将文件添加到 .gitignore
echo "要删除的文件名" >> .gitignore

# 2. 从git索引中移除文件
git rm --cached 文件名

# 3. 提交更改
git commit -m "从版本控制移除文件"

# 4. 推送到远程
git push origin dev
```

```bash
# 从暂存区移除特定文件
git reset HEAD 文件名

# 或者移除所有暂存的文件
git reset HEAD

# 然后提交并推送
git commit -m "移除文件"
git push origin 分支名
```

```bash
# 检查文件状态
git status

# 查看远程文件是否已删除
git ls-remote origin

# 撤销提交和暂存回到git add之前的状态，修改仅存在于工作区
git reset --mixed HEAD~1
```


```bash
.venv\Scripts\activate && python -m src.web.scripts.init_admin

cd web-ui && npm run dev

.venv\Scripts\activate && python test_web_api.py

.venv\Scripts\activate && python start_web.py
```

刚才网络断开，请接着最后内容继续

帮我继续优化Web

运行lint检查，确保所有修改都没有语法错误

修复这些lint错误

ENTER RESEARCH MODE

ENTER PLAN MODE

ENTER EXECUTE MODE

ENTER PLAN MODE，状态：执行成功，帮我继续优化Web

python start_web.py

如果队列满时能否进行动态扩容机制呢而不是阻塞等待呢



ENTER PLAN MODE，请根据上述RESEARCH制定优化方案。补充：1.写入频率用定时触发，每分钟触发一次写入，而不是固定tick条数。2.文件锁定方案：方案1：threading.Lock（进程内锁）。3.数据去重：选项B：追加后定期去重，在非交易时间（如每日16:00收盘后）统一去重，配合压缩流程：去重 → 排序 → 压缩为tar.gz

多周期K线合成器初始化也打印进度

ENTER EXECUTE MODE，还要补充一点，提前刷新的数据刷到文件或slqite后要及时清理已刷新的数据（但刷新后新产生的数据不要清理防止丢失数据），防止缓冲区数据积压造成多次频繁触及阈值。

另外继续优化tick数据存储结构，字段表头顺序改为：TradingDay、ExchangeID、LastPrice、PreSettlementPrice、PreClosePrice、PreOpenInterest、OpenPrice、HighestPrice、LowestPrice、Volume、Turnover、OpenInterest、ClosePrice、SettlementPrice、UpperLimitPrice、LowerLimitPrice、PreDelta、CurrDelta、UpdateTime、UpdateMillisec、BidPrice1、BidVolume1、AskPrice1、AskVolume1、BidPrice2、BidVolume2、AskPrice2、AskVolume2、BidPrice3、BidVolume3、AskPrice3、AskVolume3、BidPrice4、BidVolume4、AskPrice4、AskVolume4、BidPrice5、BidVolume5、AskPrice5、AskVolume5、AveragePrice、ActionDay、InstrumentID、ExchangeInstID、BandingUpperPrice、BandingLowerPrice、Timestamp，去掉之前无用的字段。csv字段顺序同sqlite顺序一致，sqlite中加入自增ID作为索引键。同时上一步的Kline字段顺序调整为：BarType、TradingDay、UpdateTime、InstrumentID、ExchangeID、Volume、OpenInterest、OpenPrice、HighestPrice、LowestPrice、ClosePrice、LastVolume、Timestamp并加入自增ID作为索引键