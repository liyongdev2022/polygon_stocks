package config

type Config struct {
	ApiInfo   ApiInfo   `yaml:"apiInfo"   json:"apiInfo"`
	StockInfo StockInfo `yaml:"stockInfo" json:"stockInfo"`
	LogInfo   LogInfo   `yaml:"logInfo"   json:"logInfo"`
	MongoInfo MongoInfo `yaml:"mongoInfo" json:"mongoInfo"`
}
