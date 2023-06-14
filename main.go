package main

import (
	"fmt"
	"github.com/go-co-op/gocron"
	"github.com/spf13/viper"
	"polygon_stocks/config"
	"time"
)

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

	timezone, _ := time.LoadLocation("Asia/Shanghai")
	s := gocron.NewScheduler(timezone)

	// 每秒执行一次
	//s.Every(2).Seconds().Do(func() {
	//	go cron1()
	//})

	//// 每秒执行一次
	//s.Every(1).Second().Do(func() {
	//	go cron2()
	//})

	s.StartBlocking()

}

func cron1() {
	fmt.Println("cron1")
}

func cron2() {
	fmt.Println("cron2")
}
