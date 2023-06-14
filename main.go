package main

import (
	"context"
	"fmt"
	"github.com/go-co-op/gocron"
	"github.com/golang-module/carbon"
	polygon "github.com/polygon-io/client-go/rest"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"

	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"os"
	"polygon_stocks/config"
	"time"
)

const (
	polygonBaseURL = "https://api.polygon.io"
	polygonAPIKey  = "A8e0FPkqfm5oaKJ_YDBPI_ZYMWMyS8y5"
)

type Market struct {
	Name       string
	TimeZone   string
	OpenTime   time.Time
	CloseTime  time.Time
	Frequency  int
	Collection string
}

type Stock struct {
	Ticker string
}

func main() {
	//导入配置文件
	viper.SetConfigType("yaml")
	viper.SetConfigFile("./config.yaml")
	//读取配置文件
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Errorf("read yaml error:%v", err.Error())
		return
	}
	var _config *config.Config
	//将配置文件读到结构体中
	err = viper.Unmarshal(&_config)
	if err != nil {
		fmt.Errorf("viper unmarshal yaml err:%v", err.Error())
		return
	}

	fmt.Println("apiKey==>", _config.ApiInfo.ApiKey)
	fmt.Println("------------------------------------------------")
	fmt.Println("market==>", _config.StockInfo.Market)
	fmt.Println("ticker==>", _config.StockInfo.Ticker)
	fmt.Println("beginDate==>", _config.StockInfo.BeginDate)
	fmt.Println("endDate==>", _config.StockInfo.EndDate)
	fmt.Println("frequency==>", _config.StockInfo.Frequency)
	fmt.Println("timeZone==>", _config.StockInfo.TimeZone)
	fmt.Println("------------------------------------------------")
	fmt.Println("logFile==>", _config.LogInfo.LogFile)
	fmt.Println("------------------------------------------------")
	fmt.Println("mongoURL==>", _config.MongoInfo.MongoURL)
	fmt.Println("mongoDB==>", _config.MongoInfo.MongoDB)

	// 初始化日志记录器
	logger := initLogger(_config.LogInfo.LogFile)
	defer logger.Writer()

	// 创建MongoDB客户端
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(_config.MongoInfo.MongoURL))
	if err != nil {
		logger.Fatal(err)
	}
	defer client.Disconnect(context.TODO())

	// 创建Polygon客户端
	polygonClient := polygon.New(_config.ApiInfo.ApiKey)

	// 设置股市信息
	markets := []Market{
		{
			Name:       "北美股市",
			TimeZone:   "America/New_York",
			OpenTime:   parseTime("09:30", "America/New_York"),
			CloseTime:  parseTime("16:00", "America/New_York"),
			Frequency:  5,        // 5分钟数据
			Collection: "stocks", // 存储到MongoDB的集合名称
		},
		// 添加其他股市信息
	}

	// 设置股票信息
	stocks := []Stock{
		{
			Ticker: "AAPL",
		},
		// 添加其他股票信息
	}

	// 设置时间范围
	beginDate := time.Date(2023, time.June, 1, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2023, time.June, 13, 0, 0, 0, 0, time.UTC)

	// 创建一个错误组
	eg, ctx := errgroup.WithContext(context.Background())

	// 开始抓取数据
	for _, market := range markets {
		openTime := market.OpenTime.In(market.OpenTime.Location())
		closeTime := market.CloseTime.In(market.CloseTime.Location())
		if time.Now().Before(openTime) || time.Now().After(closeTime) {
			continue // 非开市时间跳过
		}

		for _, stock := range stocks {
			market1 := market
			stock1 := stock

			eg.Go(func() error {
				return fetchData(ctx, market1, stock1, beginDate, endDate)
			})
		}
	}

	if errs := eg.Wait(); errs != nil {
		logger.Fatalf("抓取数据失败: %v", errs)
	}

	// 定时任务规则
	timezone, _ := time.LoadLocation("Asia/Shanghai")
	s := gocron.NewScheduler(timezone)

	s.StartBlocking()

}

// 抓取数据
func fetchData(ctx context.Context, market Market, stock Stock, beginDate, endDate time.Time) error {
	log.Printf("开始抓取股票数据: 市场=%s, 股票=%s\n", market.Name, stock.Ticker)

	// 抓取股票市场的开市闭市信息
	marketInfo, err := fetchMarketInfo(market.Name)
	if err != nil {
		return fmt.Errorf("获取股市信息失败: %v", err)
	}

	// 抓取股票交易信息
	for date := beginDate; date.Before(endDate); date = date.AddDate(0, 0, 1) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if date.Weekday() == time.Saturday || date.Weekday() == time.Sunday {
				continue // 跳过周末
			}

			if !isTradingDay(date, marketInfo) {
				continue // 非交易日跳过
			}

			// 构建API URL
			apiURL := fmt.Sprintf("%s/v2/aggs/ticker/%s/range/%d/minute/%s/%s?apiKey=%s",
				polygonBaseURL, stock.Ticker, market.Frequency,
				date.Format("2006-01-02"), date.AddDate(0, 0, 1).Format("2006-01-02"),
				polygonAPIKey)

			// 发送HTTP请求获取数据
			response, errs := sendHTTPRequest(apiURL)
			if errs != nil {
				return fmt.Errorf("发送HTTP请求失败: %v", errs)
			}

			// 解析响应数据
			data := gjson.Get(response, "results")

			// 将数据保存到MongoDB
			err = saveDataToMongoDB(data, market.Collection)
			if err != nil {
				return fmt.Errorf("保存数据到MongoDB失败: %v", err)
			}

			log.Printf("抓取数据成功: 市场=%s, 股票=%s, 日期=%s\n", market.Name, stock.Ticker, date.Format("2006-01-02"))
		}
	}

	return nil
}

// 抓取股票市场的开市闭市信息
func fetchMarketInfo(marketName string) (gjson.Result, error) {
	apiURL := fmt.Sprintf("%s/v3/reference/tickers?market=%s&active=true&apiKey=%s", polygonBaseURL, marketName, polygonAPIKey)

	response, err := sendHTTPRequest(apiURL)
	if err != nil {
		return gjson.Result{}, err
	}

	// 解析响应数据
	data := gjson.Get(response, "results")

	// 返回第一个市场的信息
	return data.Get("0"), nil
}

// 是否交易日
func isTradingDay(date time.Time, marketInfo gjson.Result) bool {
	marketOpenTime := marketInfo.Get("open").Int()
	marketCloseTime := marketInfo.Get("close").Int()

	openTime := carbon.CreateFromTimestamp(marketOpenTime, marketInfo.Get("timezone").Str)
	closeTime := carbon.CreateFromTimestamp(marketCloseTime, marketInfo.Get("timezone").Str)

	openTime = openTime.SetYear(date.Year())
	openTime = openTime.SetMonth(date.Month())
	openTime = openTime.SetDay(date.Day())

	closeTime = closeTime.SetYear(date.Year())
	closeTime = closeTime.SetMonth(date.Month())
	closeTime = closeTime.SetDay(date.Day())

	return date.After(openTime.Time) && date.Before(closeTime.Time)
}

// 保存到mongoDB
func saveDataToMongoDB(data gjson.Result, collectionName string) error {
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		return err
	}
	ctx := context.TODO()
	err = client.Connect(ctx)
	if err != nil {
		return err
	}
	defer client.Disconnect(ctx)

	collection := client.Database("stockdata").Collection(collectionName)

	// 处理每一条数据并保存到MongoDB
	for _, result := range data.Array() {
		item := result.Map()

		// 格式化时间戳
		timestamp := item["t"].Int()
		item["timestamp"] = timestamp
		item["local_time"] = time.Unix(timestamp/1000, 0).Format("2006-01-02 15:04:05")

		// 将数据插入MongoDB
		_, err = collection.InsertOne(ctx, item)
		if err != nil {
			return err
		}
	}

	return nil
}
func sendHTTPRequest(url string) (string, error) {
	// 使用您选择的HTTP库发送GET请求并获取响应，示例中使用伪代码代替
	response, err := sendGetRequest(url)
	if err != nil {
		return "", err
	}

	return response, nil
}

// 初始化日志记录器
func initLogger(logFile string) *log.Logger {
	file, err := os.OpenFile(logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("无法打开日志文件：%s", err.Error())
	}
	defer file.Close()
	logger := log.New(file, "", log.LstdFlags)
	return logger
}

// 解析时区
func parseTime(timeStr, timeZone string) time.Time {
	loc, err := time.LoadLocation(timeZone)
	if err != nil {
		log.Fatalf("加载时区失败: %v\n", err)
	}

	currentTime := time.Now().In(loc)
	parsedTime, err := time.ParseInLocation("15:04", timeStr, currentTime.Location())
	if err != nil {
		log.Fatalf("解析时间失败: %v\n", err)
	}

	return parsedTime
}
