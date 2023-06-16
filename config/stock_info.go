package config

type StockInfo struct {
	Market     string `yaml:"market"    json:"market"`
	Ticker     string `yaml:"ticker"    json:"ticker"`
	BeginDate  string `yaml:"beginDate" json:"beginDate"`
	EndDate    string `yaml:"endDate"   json:"endDate"`
	Multiplier int    `yaml:"multiplier" json:"multiplier"`
	Timespan   string `yaml:"timespan" json:"timespan"`
	TimeZone   string `yaml:"timeZone"  json:"timeZone"`
}
