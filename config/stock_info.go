package config

type StockInfo struct {
	Market    string `yaml:"market"    json:"market"`
	Ticker    string `yaml:"ticker"    json:"ticker"`
	BeginDate string `yaml:"beginDate" json:"beginDate"`
	EndDate   string `yaml:"endDate"   json:"endDate"`
	Frequency string `yaml:"frequency" json:"frequency"`
	TimeZone  string `yaml:"timeZone"  json:"timeZone"`
}
