package config

import "github.com/spf13/viper"

var C config

type config struct {
	TMDB struct {
		ApiKey   string `mapstructure:"apiKey"`
		Region   string `mapstructure:"region"`
		Language string `mapstructure:"language"`
	} `mapstructure:"tmdb"`
	Rates struct {
		ScrapeInterval int `mapstructure:"scrapeInterval"`
		PagesPerScrape int `mapstructure:"pagesPerScrape"`
	} `mapstructure:"rates"`
	DB struct {
		Host     string `mapstructure:"host"`
		User     string `mapstructure:"user"`
		Password string `mapstructure:"password"`
	} `mapstructure:"db"`
	Posters struct {
		BaseUrl string `mapstructure:"baseUrl"`
		BaseDir string `mapstructure:"baseDir"`
	} `mapstructure:"posters"`
}

func Load(configFilePath string) error {
	viper.SetConfigFile(configFilePath)

	if err := viper.ReadInConfig(); err != nil {
		return err
	}

	C = config{}

	if err := viper.Unmarshal(&C); err != nil {
		return err
	}

	return nil
}
