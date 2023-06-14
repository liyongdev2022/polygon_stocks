package config

type MongoInfo struct {
	MongoURL string `yaml:"mongoURL" json:"mongoURL"`
	MongoDB  string `yaml:"mongoDB"  json:"mongoDB"`
}
