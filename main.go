package main

import (
	"context"
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/go-co-op/gocron"
	polygon "github.com/polygon-io/client-go/rest"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"os"
	"polygon_stocks/config"

	"time"
)

var (
	Logger       *log.Logger
	GlobalViper  *viper.Viper
	GlobalConfig *config.Config
)

// 初始化
func init() {
	fmt.Println("init polygon stocks......")
	initConfig()
	go dynamicConfig()
}

// 初始化配置文件
func initConfig() {
	GlobalViper = viper.New()
	GlobalViper.SetConfigName("config") // 配置文件名称
	GlobalViper.AddConfigPath(".")      // 从当前目录的哪个文件开始查找
	GlobalViper.SetConfigType("yaml")   // 配置文件的类型
	err := GlobalViper.ReadInConfig()   // 读取配置文件
	if err != nil {                     // 可以按照这种写法，处理特定的找不到配置文件的情况
		if v, ok := err.(viper.ConfigFileNotFoundError); ok {
			fmt.Println(v)
		} else {
			panic(fmt.Errorf("read config err:%v\n", err))
		}
	}
	err = GlobalViper.Unmarshal(&GlobalConfig)
	if err != nil {
		panic(fmt.Errorf("viper unmarshal yaml err:%v", err.Error()))
	}
	fmt.Println("apiKey==>", GlobalConfig.ApiInfo.ApiKey)
	fmt.Println("------------------------------------------------")
	fmt.Println("market==>", GlobalConfig.StockInfo.Market)
	fmt.Println("ticker==>", GlobalConfig.StockInfo.Ticker)
	fmt.Println("beginDate==>", GlobalConfig.StockInfo.BeginDate)
	fmt.Println("endDate==>", GlobalConfig.StockInfo.EndDate)
	fmt.Println("frequency==>", GlobalConfig.StockInfo.Frequency)
	fmt.Println("timeZone==>", GlobalConfig.StockInfo.TimeZone)
	fmt.Println("------------------------------------------------")
	fmt.Println("logFile==>", GlobalConfig.LogInfo.LogFile)
	fmt.Println("------------------------------------------------")
	fmt.Println("mongoURL==>", GlobalConfig.MongoInfo.MongoURL)
	fmt.Println("mongoDB==>", GlobalConfig.MongoInfo.MongoDB)

}

// viper支持应用程序在运行中实时读取配置文件的能力。确保在调用 WatchConfig()之前添加所有的configPaths。
func dynamicConfig() {
	GlobalViper.WatchConfig()
	GlobalViper.OnConfigChange(func(event fsnotify.Event) {
		fmt.Printf("发现配置信息发生变化: %s\n", event.String())

		fmt.Println("endData==>", GlobalConfig.StockInfo.EndDate)

	})
}

func main() {

	defer func() {
		if err := recover(); err != nil {
			Logger.Println("panic err:", err)
		}
	}()

	// 初始化日志记录器
	Logger = initLogger(GlobalConfig.LogInfo.LogFile)
	defer Logger.Writer()

	// 创建MongoDB客户端
	mongo_client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(GlobalConfig.MongoInfo.MongoURL))
	if err != nil {
		Logger.Fatal(err)
	}
	defer mongo_client.Disconnect(context.TODO())

	// 创建Polygon客户端
	polygon_client := polygon.New(GlobalConfig.ApiInfo.ApiKey)

	// 开始抓取数据
	_ = polygon_client

	// 定时任务规则
	timezone, _ := time.LoadLocation("Asia/Shanghai")
	s := gocron.NewScheduler(timezone)

	s.StartBlocking()

}

// 抓取数据
//func fetchData(ctx context.Context, market Market, stock Stock, beginDate, endDate time.Time) error {
//	log.Printf("开始抓取股票数据: 市场=%s, 股票=%s\n", market.Name, stock.Ticker)
//
//	// 抓取股票市场的开市闭市信息
//	marketInfo, err := fetchMarketInfo(market.Name)
//	if err != nil {
//		return fmt.Errorf("获取股市信息失败: %v", err)
//	}
//
//	// 抓取股票交易信息
//	for date := beginDate; date.Before(endDate); date = date.AddDate(0, 0, 1) {
//		select {
//		case <-ctx.Done():
//			return ctx.Err()
//		default:
//			if date.Weekday() == time.Saturday || date.Weekday() == time.Sunday {
//				continue // 跳过周末
//			}
//
//			if !isTradingDay(date, marketInfo) {
//				continue // 非交易日跳过
//			}
//
//			// 构建API URL
//			apiURL := fmt.Sprintf("%s/v2/aggs/ticker/%s/range/%d/minute/%s/%s?apiKey=%s",
//				polygonBaseURL, stock.Ticker, market.Frequency,
//				date.Format("2006-01-02"), date.AddDate(0, 0, 1).Format("2006-01-02"),
//				polygonAPIKey)
//
//			// 发送HTTP请求获取数据
//			response, errs := sendHTTPRequest(apiURL)
//			if errs != nil {
//				return fmt.Errorf("发送HTTP请求失败: %v", errs)
//			}
//
//			// 解析响应数据
//			data := gjson.Get(response, "results")
//
//			// 将数据保存到MongoDB
//			err = saveDataToMongoDB(data, market.Collection)
//			if err != nil {
//				return fmt.Errorf("保存数据到MongoDB失败: %v", err)
//			}
//
//			log.Printf("抓取数据成功: 市场=%s, 股票=%s, 日期=%s\n", market.Name, stock.Ticker, date.Format("2006-01-02"))
//		}
//	}
//
//	return nil
//}
//
//// 抓取股票市场的开市闭市信息
//func fetchMarketInfo(marketName string) (gjson.Result, error) {
//	apiURL := fmt.Sprintf("%s/v3/reference/tickers?market=%s&active=true&apiKey=%s", polygonBaseURL, marketName, polygonAPIKey)
//
//	response, err := sendHTTPRequest(apiURL)
//	if err != nil {
//		return gjson.Result{}, err
//	}
//
//	// 解析响应数据
//	data := gjson.Get(response, "results")
//
//	// 返回第一个市场的信息
//	return data.Get("0"), nil
//}

//
//// 是否交易日
//func isTradingDay(date time.Time, marketInfo gjson.Result) bool {
//	marketOpenTime := marketInfo.Get("open").Int()
//	marketCloseTime := marketInfo.Get("close").Int()
//
//	openTime := carbon.CreateFromTimestamp(marketOpenTime, marketInfo.Get("timezone").Str)
//	closeTime := carbon.CreateFromTimestamp(marketCloseTime, marketInfo.Get("timezone").Str)
//
//	openTime = openTime.SetYear(date.Year())
//	openTime = openTime.SetMonth(date.Month())
//	openTime = openTime.SetDay(date.Day())
//
//	closeTime = closeTime.SetYear(date.Year())
//	closeTime = closeTime.SetMonth(date.Month())
//	closeTime = closeTime.SetDay(date.Day())
//
//	return date.After(openTime.Time) && date.Before(closeTime.Time)
//}
//
//// 保存到mongoDB
//func saveDataToMongoDB(data gjson.Result, collectionName string) error {
//	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017"))
//	if err != nil {
//		return err
//	}
//	ctx := context.TODO()
//	err = client.Connect(ctx)
//	if err != nil {
//		return err
//	}
//	defer client.Disconnect(ctx)
//
//	collection := client.Database("stockdata").Collection(collectionName)
//
//	// 处理每一条数据并保存到MongoDB
//	for _, result := range data.Array() {
//		item := result.Map()
//
//		// 格式化时间戳
//		timestamp := item["t"].Int()
//		item["timestamp"] = timestamp
//		item["local_time"] = time.Unix(timestamp/1000, 0).Format("2006-01-02 15:04:05")
//
//		// 将数据插入MongoDB
//		_, err = collection.InsertOne(ctx, item)
//		if err != nil {
//			return err
//		}
//	}
//
//	return nil
//}
//func sendHTTPRequest(url string) (string, error) {
//	// 使用您选择的HTTP库发送GET请求并获取响应，示例中使用伪代码代替
//	//response, err := sendGetRequest(url)
//	//if err != nil {
//	//	return "", err
//	//}
//	//
//	//return response, nil
//
//	return "", nil
//}

// 初始化日志记录器
func initLogger(logFile string) *log.Logger {
	file, err := os.OpenFile(logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("无法打开日志文件：%s", err.Error())
	}
	defer file.Close()
	loggers := log.New(file, "", log.LstdFlags)
	return loggers
}

// 解析时区
func parseTime(timeStr, timeZone string) time.Time {
	loc, err := time.LoadLocation(timeZone)
	if err != nil {
		Logger.Fatalf("加载时区失败: %v\n", err)
	}

	currentTime := time.Now().In(loc)
	parsedTime, err := time.ParseInLocation("15:04", timeStr, currentTime.Location())
	if err != nil {
		Logger.Fatalf("解析时间失败: %v\n", err)
	}

	return parsedTime
}
