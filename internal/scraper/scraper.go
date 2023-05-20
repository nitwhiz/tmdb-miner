package scraper

import (
	"context"
	"github.com/nitwhiz/tmdb-scraper/internal/config"
	"github.com/nitwhiz/tmdb-scraper/internal/poster"
	"github.com/ryanbradynd05/go-tmdb"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

func runScrape(tmdbAPI *tmdb.TMDb, db *mongo.Database, pf *poster.Fetcher) error {
	log.Info("begin scrape")

	if err := FetchMovieGenres(tmdbAPI, db); err != nil {
		return err
	}

	if err := FetchTvSeriesGenres(tmdbAPI, db); err != nil {
		return err
	}

	if err := FetchMovies(tmdbAPI, db, pf, config.C.Rates.PagesPerScrape); err != nil {
		return err
	}

	if err := FetchTvSeries(tmdbAPI, db, pf, config.C.Rates.PagesPerScrape); err != nil {
		return err
	}

	log.Info("scrape finished")

	return nil
}

func Start(tmdbAPI *tmdb.TMDb, db *mongo.Database, pf *poster.Fetcher) (context.Context, context.CancelFunc) {
	scrapeInterval := time.Minute * time.Duration(config.C.Rates.ScrapeInterval)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(scrapeInterval):
				if err := runScrape(tmdbAPI, db, pf); err != nil {
					log.Error(err)
				}

				break
			}
		}
	}()

	return ctx, cancel
}
