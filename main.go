package main

import (
	"context"
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/go-co-op/gocron"
	polygon "github.com/polygon-io/client-go/rest"
	"github.com/polygon-io/client-go/rest/models"
	"github.com/spf13/viper"
	"github.com/vito-go/mcache"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"os"
	"polygon_stocks/config"
	"strconv"
	"strings"
	"sync"
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
	//fmt.Println("apiKey==>", GlobalConfig.ApiInfo.ApiKey)
	//fmt.Println("------------------------------------------------")
	//fmt.Println("market==>", GlobalConfig.StockInfo.Market)
	//fmt.Println("ticker==>", GlobalConfig.StockInfo.Ticker)
	//fmt.Println("beginDate==>", GlobalConfig.StockInfo.BeginDate)
	//fmt.Println("endDate==>", GlobalConfig.StockInfo.EndDate)
	//fmt.Println("Multiplier==>", GlobalConfig.StockInfo.Multiplier)
	//fmt.Println("timespan==>", GlobalConfig.StockInfo.Timespan)
	//fmt.Println("timeZone==>", GlobalConfig.StockInfo.TimeZone)
	//fmt.Println("------------------------------------------------")
	//fmt.Println("logFile==>", GlobalConfig.LogInfo.LogFile)
	//fmt.Println("------------------------------------------------")
	//fmt.Println("mongoURL==>", GlobalConfig.MongoInfo.MongoURL)
	//fmt.Println("mongoDB==>", GlobalConfig.MongoInfo.MongoDB)

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
			Logger.Fatalf("panic err:", err)
		}
	}()

	// 初始化日志记录器
	Logger = initLogger(GlobalConfig.LogInfo.LogFile)
	defer Logger.Writer()

	// 创建MongoDB客户端
	mongo_client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(GlobalConfig.MongoInfo.MongoURL))
	if err != nil {
		Logger.Fatalf("create mongo client err:%v\n", err)
	}

	// 创建Polygon客户端
	polygon_client := polygon.New(GlobalConfig.ApiInfo.ApiKey)

	date, _ := time.Parse("2006-01-02", GlobalConfig.StockInfo.BeginDate)
	fmt.Println("newDate==>", date)
	params := models.ListAggsParams{
		Ticker:     GlobalConfig.StockInfo.Ticker,
		Multiplier: GlobalConfig.StockInfo.Multiplier,
		Timespan:   models.Timespan(GlobalConfig.StockInfo.Timespan),
		From:       models.Millis(time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)),
		To:         models.Millis(time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)),
	}.WithOrder(models.Desc).WithLimit(50000).WithAdjusted(true)

	iter := polygon_client.ListAggs(context.Background(), params)
	for iter.Next() {
		log.Print(iter.Item())
	}
	if iter.Err() != nil {
		log.Fatal(iter.Err())
	}
	return

	// 开始抓取数据
	var wg sync.WaitGroup
	if len(GlobalConfig.StockInfo.Ticker) > 0 {
		for _, ticker := range strings.Split(GlobalConfig.StockInfo.Ticker, ",") {
			wg.Add(1)
			go func(ticker string) {
				defer wg.Done()
				// 开始抓取
				err = fetchData(polygon_client, mongo_client, GlobalConfig.StockInfo.Market, ticker, GlobalConfig.StockInfo.BeginDate, GlobalConfig.StockInfo.EndDate, GlobalConfig.StockInfo.Multiplier, GlobalConfig.StockInfo.Timespan, GlobalConfig.StockInfo.TimeZone)
				if err != nil {
					Logger.Fatalf("Failed to fetch data for ticker %s: %v\n", ticker, err)
				}
			}(ticker)
		}
		wg.Wait()
	} else {
		fmt.Println("爬取当前股市的所有股票交易信息")
	}

	// 定时任务规则
	timezone, _ := time.LoadLocation("Asia/Shanghai")
	s := gocron.NewScheduler(timezone)

	s.StartBlocking()

}

// 抓取数据
func fetchData(client *polygon.Client, mongoClient *mongo.Client, market, ticker, beginDateStr, endDateStr string, multiplier int, timespan, timezone string) error {
	Logger.Printf("开始抓取股票数据: 市场=%s, 股票=%s\n", market, ticker)

	beginDate, _ := time.Parse("2006-01-02", beginDateStr)
	endDate, _ := time.Parse("2006-01-02", endDateStr)

	activeCh := make(chan bool, 1)
	finshCh := make(chan bool, 1)
	// 抓取股票交易信息
	for date := beginDate; date.Before(endDate) || date.Equal(endDate); date = date.AddDate(0, 0, 1) {
		// 获取日期当天股票详情信息
		go func(cha chan<- bool) {
			params := models.GetTickerDetailsParams{
				Ticker: ticker,
			}.WithDate(models.Date(time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)))
			details, err := client.ReferenceClient.GetTickerDetails(context.Background(), params)
			if err != nil {
				Logger.Printf("get ticker details err:%v\n", err)
				return
			}
			err = saveTickerDataToMongoDB(details, mongoClient, "stock_tickers_history")
			if err != nil {
				Logger.Printf("save stock ticker history data err:%v\n", err)
			}
			if details != nil {
				cha <- details.Results.Active
			}

		}(activeCh)
		fmt.Println("wait.....", date.Format("2006-01-02"))
		fmt.Println("历史交易->执行完了")

		if <-activeCh {
			go func(chf chan<- bool) {
				params := models.ListAggsParams{
					Ticker:     ticker,
					Multiplier: multiplier,
					Timespan:   models.Timespan(timespan),
					From:       models.Millis(time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)),
					To:         models.Millis(time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)),
				}.WithOrder(models.Desc).WithLimit(50000).WithAdjusted(true)

				iter := client.ListAggs(context.Background(), params)
				for iter.Next() {
					log.Print(iter.Item())
				}
				if iter.Err() != nil {
					log.Fatal(iter.Err())
				}
				chf <- true

			}(finshCh)
		}

		<-finshCh

		fmt.Println("股票交易->执行完了")

	}

	return nil
}

// 删除抓取不完整的数据
func deleteDataForMongoDB(ticker string, date string, mongoClient *mongo.Client, collectionName string) error {
	collection := mongoClient.Database(GlobalConfig.MongoInfo.MongoDB).Collection(collectionName)
	_, err := collection.DeleteMany(context.TODO(), bson.D{{"ticker", ticker}, {"trade_date", date}})
	if err != nil {
		return err
	}
	return nil
}

// 保存股票交易信息
func saveDataToMongoDB(data models.Agg, ticker string, mongoClient *mongo.Client, collectionName string) error {
	collection := mongoClient.Database(GlobalConfig.MongoInfo.MongoDB).Collection(collectionName)
	ts, err := data.Timestamp.MarshalJSON()
	if err != nil {
		return err
	}
	intTs, err := strconv.ParseInt(string(ts), 10, 64)
	if err != nil {
		return err
	}
	location, err := time.LoadLocation(GlobalConfig.StockInfo.TimeZone)
	if err != nil {
		return err
	}
	_, err = collection.InsertOne(context.TODO(), bson.M{
		"ticker":          ticker,
		"timestamp":       intTs,
		"open":            data.Open,
		"high":            data.High,
		"low":             data.Low,
		"close":           data.Close,
		"volume":          data.Volume,
		"volume_weighted": data.VWAP,
		"trade_date":      time.UnixMilli(intTs).UTC().In(location).Format("2006-01-02 15:04:05"),
	})
	return err
}

// 保存股票历史交易信息
func saveTickerDataToMongoDB(data *models.GetTickerDetailsResponse, mongoClient *mongo.Client, collectionName string) error {
	collection := mongoClient.Database(GlobalConfig.MongoInfo.MongoDB).Collection(collectionName)
	_, err := collection.InsertOne(context.TODO(), bson.M{
		"ticker":           data.Results.Ticker,
		"company_name":     data.Results.Name,
		"primary_exchange": data.Results.PrimaryExchange,
		"active":           data.Results.Active,
		"last_updated_utc": data.Results.LastUpdatedUTC,
		"currency_name":    data.Results.CurrencyName,
		"locale":           data.Results.Locale,
		"cik":              data.Results.CIK,
		"composite_figi":   data.Results.CompositeFIGI,
		"share_class_figi": data.Results.ShareClassFIGI,
	})
	return err
}

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

func convertTimezone(timestamp int64, timezone string) (time.Time, error) {
	// 创建一个时间对象，使用给定的时间戳和 UTC 时区
	utcTime := time.Unix(timestamp, 0).UTC()

	// 解析指定的时区
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		return time.Time{}, err
	}

	// 将时间对象转换为指定时区
	timeInZone := utcTime.In(loc)

	return timeInZone, nil
}

// 解析时区
func parseTime(timeStr, timeZone string) time.Time {
	loc, err := time.LoadLocation(timeZone)
	if err != nil {
		fmt.Printf("加载时区失败: %v\n", err)
	}

	currentTime := time.Now().In(loc)
	parsedTime, err := time.ParseInLocation("2006-01-02 15:04:05", timeStr, currentTime.Location())
	if err != nil {
		fmt.Printf("解析时间失败: %v\n", err)
	}

	return parsedTime
}

// 设置本地缓存
func setLocalCache(ticker string, key string, value string) error {
	c, err := mcache.NewMcache(ticker)
	if err != nil {
		return err
	}
	err = c.Set(key, []byte(value))
	return err

}

// 获取本地缓存
func getLocalCache(ticker string, key string) (string, error) {
	c, err := mcache.NewMcache(ticker)
	if err != nil {
		return "", err
	}
	return string(c.Get(key)), nil
}
