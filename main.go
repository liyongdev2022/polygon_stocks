package main

import (
	"context"
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/go-co-op/gocron"
	polygon "github.com/polygon-io/client-go/rest"
	"github.com/polygon-io/client-go/rest/models"
	"github.com/spf13/viper"
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

type StockData struct {
	Market string    `json:"market"`
	Ticker string    `json:"ticker"`
	Time   time.Time `json:"time"`
}

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
	fmt.Println("Multiplier==>", GlobalConfig.StockInfo.Multiplier)
	fmt.Println("timespan==>", GlobalConfig.StockInfo.Timespan)
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

	// 获取股市开市闭市信息
	marketStatus, err := getMarketStatus(polygon_client)
	if err != nil {
		Logger.Fatalf("get market status err:%v", err)
	}

	// 开始抓取数据
	var wg sync.WaitGroup
	if len(GlobalConfig.StockInfo.Ticker) > 0 {
		for _, ticker := range strings.Split(GlobalConfig.StockInfo.Ticker, ",") {
			wg.Add(1)
			go func(ticker string) {
				defer wg.Done()
				// 开始抓取
				err = fetchData(context.TODO(), polygon_client, mongo_client, marketStatus, GlobalConfig.StockInfo.Market, ticker, GlobalConfig.StockInfo.BeginDate, GlobalConfig.StockInfo.EndDate, GlobalConfig.StockInfo.Multiplier, GlobalConfig.StockInfo.Timespan, GlobalConfig.StockInfo.TimeZone)
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
func fetchData(ctx context.Context, client *polygon.Client, mongoClient *mongo.Client, marketStatus *models.GetMarketStatusResponse, market, ticker, beginDateStr, endDateStr string, multiplier int, timespan, timezone string) error {
	Logger.Printf("开始抓取股票数据: 市场=%s, 股票=%s\n", market, ticker)

	beginDate, _ := time.Parse("2006-01-02", beginDateStr)
	endDate, _ := time.Parse("2006-01-02", endDateStr)

	activeCh := make(chan bool)

	// 抓取股票交易信息
	for date := beginDate; date.Before(endDate); {

		// 获取日期当天股票详情信息
		go func(activeCh chan bool) {
			params := models.GetTickerDetailsParams{
				Ticker: ticker,
			}.WithDate(models.Date(date))
			details, err := client.ReferenceClient.GetTickerDetails(context.TODO(), params)
			if err != nil {
				Logger.Printf("get ticker details err:%v\n", err)
				return
			}
			if details != nil {
				activeCh <- details.Results.Active
			}
			err = saveTickerDataToMongoDB(details, mongoClient, "stock_tickers_history")
			if err != nil {
				Logger.Printf("save stock ticker history data err:%v\n", err)
			}

		}(activeCh)

		// 获取日期当天股票交易数据
		go func(activeCh chan bool) {
			for {
				select {
				case active := <-activeCh:
					if active {
						fmt.Printf("ticker:%s,data:%v,active:%v\n", ticker, date, active)
						params := models.ListAggsParams{
							Ticker:     ticker,
							From:       models.Millis(beginDate),
							To:         models.Millis(endDate),
							Multiplier: multiplier,
							Timespan:   models.Timespan(timespan),
						}.WithOrder(models.Desc).WithAdjusted(true).WithLimit(50000)

						iter := client.ListAggs(context.Background(), params)
						for iter.Next() {
							log.Print(iter.Item())
							err := saveDataToMongoDB(iter.Item(), ticker, mongoClient, "stock_data_"+strconv.Itoa(multiplier)+timespan)
							if err != nil {
								Logger.Printf("save ticker details data err:%v\n", err)
							}
						}
						if iter.Err() != nil {
							log.Fatal(iter.Err())
						}

					}
				}
			}

		}(activeCh)

		date = date.AddDate(0, 0, 1)
	}

	return nil
}
func saveDataToMongoDB(data models.Agg, ticker string, mongoClient *mongo.Client, collectionName string) error {
	collection := mongoClient.Database(GlobalConfig.MongoInfo.MongoDB).Collection(collectionName)
	//ts, err := data.Timestamp.MarshalJSON()
	//if err != nil {
	//	fmt.Println("ts marshal errs==>", err)
	//	return err
	//}
	//fmt.Println("ts==>", string(ts))
	//
	//fmt.Println("now==>", time.Now())
	//fmt.Println("utc==>", time.Now().UTC())
	//location, _ := time.LoadLocation(GlobalConfig.StockInfo.TimeZone)
	//
	//fmt.Println("am===>", time.Now().UTC().In(location))

	_, err := collection.InsertOne(context.TODO(), bson.M{
		"ticker":          data.Ticker,
		"timestamp":       data.Timestamp,
		"open":            data.Open,
		"high":            data.High,
		"low":             data.Low,
		"close":           data.Close,
		"volume":          data.Volume,
		"volume_weighted": data.VWAP,
		"trade_date":      "",
	})
	return err
}

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

// 获取股市开市闭市信息
func getMarketStatus(client *polygon.Client) (*models.GetMarketStatusResponse, error) {
	marketStatus, err := client.ReferenceClient.GetMarketStatus(context.Background(), models.RequestOption(func(o *models.RequestOptions) {}))
	if err != nil {
		return nil, err
	}
	return marketStatus, nil
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

// 获取上次处理的时间
func getLastProcessedTime(ctx context.Context, client *mongo.Client, market, ticker string) (time.Time, error) {
	collection := client.Database("stock_data").Collection("last_processed_data")

	filter := bson.M{"market": market, "ticker": ticker}
	options := options.FindOne().SetSort(bson.M{"time": -1})

	result := collection.FindOne(ctx, filter, options)
	if result.Err() != nil {
		if result.Err() == mongo.ErrNoDocuments {
			return time.Time{}, nil // 没有找到记录，返回零时间
		}
		return time.Time{}, result.Err()
	}

	var data StockData
	err := result.Decode(&data)
	if err != nil {
		return time.Time{}, err
	}

	return data.Time, nil
}

func updateLastProcessedTime(ctx context.Context, client *mongo.Client, market, ticker string, time time.Time) error {
	collection := client.Database("stock_data").Collection("last_processed_data")

	filter := bson.M{"market": market, "ticker": ticker}
	update := bson.M{"$set": bson.M{"time": time.Unix()}}
	options := options.Update().SetUpsert(true)

	_, err := collection.UpdateOne(ctx, filter, update, options)
	if err != nil {
		return err
	}

	return nil
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
