package main

import (
	"context"
	"github.com/nitwhiz/tmdb-miner/internal/poster"
	"github.com/nitwhiz/tmdb-miner/internal/scraper"
	"github.com/ryanbradynd05/go-tmdb"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

func main() {
	viper.SetConfigFile(".env")

	if err := viper.ReadInConfig(); err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://root:root@localhost:27017"))

	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()

	var tmdbAPI *tmdb.TMDb

	config := tmdb.Config{
		APIKey:   viper.GetString("TMDB_API_KEY"),
		Proxies:  nil,
		UseProxy: false,
	}

	tmdbAPI = tmdb.Init(config)

	db := client.Database("tmdb")

	pf := poster.NewFetcher()

	if err := scraper.FetchMovieGenres(tmdbAPI, db); err != nil {
		panic(err)
	}

	if err := scraper.FetchTvSeriesGenres(tmdbAPI, db); err != nil {
		panic(err)
	}

	if err := scraper.FetchMovies(tmdbAPI, db, pf, viper.GetInt("PAGES_PER_FETCH")); err != nil {
		panic(err)
	}

	if err := scraper.FetchTvSeries(tmdbAPI, db, pf, viper.GetInt("PAGES_PER_FETCH")); err != nil {
		panic(err)
	}

	log.Info("done!")
}
