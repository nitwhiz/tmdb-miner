package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/nitwhiz/tmdb-scraper/internal/config"
	"github.com/nitwhiz/tmdb-scraper/internal/poster"
	"github.com/nitwhiz/tmdb-scraper/internal/scraper"
	"github.com/ryanbradynd05/go-tmdb"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

func main() {
	configFlag := flag.String("config", "/config.yml", "config file path")

	if err := config.Load(*configFlag); err != nil {
		log.Error(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	defer cancel()

	client, err := mongo.Connect(
		ctx,
		options.Client().ApplyURI(
			fmt.Sprintf(
				"mongodb://%s:%s@%s",
				config.C.DB.User,
				config.C.DB.Password,
				config.C.DB.Host,
			),
		),
	)

	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()

	var tmdbAPI *tmdb.TMDb

	tmdbConfig := tmdb.Config{
		APIKey:   config.C.TMDB.ApiKey,
		Proxies:  nil,
		UseProxy: false,
	}

	tmdbAPI = tmdb.Init(tmdbConfig)

	db := client.Database("tmdb")

	pf := poster.NewFetcher()

	log.Info("scraper ready!")

	scraperCtx, scraperCancel := scraper.Start(tmdbAPI, db, pf)

	defer scraperCancel()

	<-scraperCtx.Done()
}
