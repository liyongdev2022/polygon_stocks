# polygon_stocks
爬取股票历史数据
ApiInfo:
# 第三方平台ApiKey
apiKey: "A8e0FPkqfm5oaKJ_YDBPI_ZYMWMyS8y5"

StockInfo:
-
# 股市，例如北美股市
market: "北美股市1" #
# 股票名称，多个逗号分开，不指定则标识抓取本股市下的所有股票
ticker: APP1 GOOGLE1
# 开始日期
beginDate: "2023-01-15"
# 结束日期
endDate: "2023-02-05"
# 数据频率 例如5分钟、10分钟
frequency: "5Min"
# 股票所在时区 Asia/Shanghai
timeZone: "America/New_York"
-
# 股市，例如北美股市
market: "北美股市2"
# 股票名称，多个逗号分开，不指定则标识抓取本股市下的所有股票
ticker: APP2 GOOGLE2
# 开始日期
beginDate: "2023-03-15"
# 结束日期
endDate: "2023-04-05"
# 数据频率 例如5分钟、10分钟
frequency: "10Min"
# 股票所在时区 Asia/Shanghai
timeZone: "America/New_York"

LogInfo:
# 日志文件路径
logFile: "log.txt"

MongoInfo:
# MongoDB连接URL
mongoURL: "mongodb://127.0.0.1:27017"
# MongoDB数据库名称
mongoDB: "stock_data"