package scraper

import (
	"context"
	"fmt"
	"github.com/nitwhiz/tmdb-scraper/internal/config"
	"github.com/nitwhiz/tmdb-scraper/internal/poster"
	"github.com/ryanbradynd05/go-tmdb"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"sync"
	"time"
)

var requestCounter = float64(0)
var secondsRunning = float64(0)

type rpsLogger struct {
	formatter log.Formatter
}

func (l rpsLogger) Format(entry *log.Entry) ([]byte, error) {
	if secondsRunning == 0 {
		entry.Data["rps"] = "0"
	} else {
		entry.Data["rps"] = fmt.Sprintf("%.2f", requestCounter/secondsRunning)
	}

	return l.formatter.Format(entry)
}

func runScrape(tmdbAPI *tmdb.TMDb, db *mongo.Database, pf *poster.Fetcher) error {
	log.SetFormatter(rpsLogger{
		formatter: log.StandardLogger().Formatter,
	})

	log.Info("begin scrape")

	if err := FetchMovieGenres(tmdbAPI, db); err != nil {
		return err
	}

	if err := FetchTvSeriesGenres(tmdbAPI, db); err != nil {
		return err
	}

	requestCounter = float64(0)
	secondsRunning = float64(0)

	wg := &sync.WaitGroup{}

	go func() {
		for {
			select {
			case <-time.After(10 * time.Millisecond):
				if secondsRunning >= 10 {
					requestCounter = 0
					secondsRunning = .01
				} else {
					secondsRunning += .01
				}
			}
		}
	}()

	csm, err := GetChangedMovieIds(tmdbAPI)

	if err != nil {
		return err
	}

	cst, err := GetChangedTvIds(tmdbAPI)

	if err != nil {
		return err
	}

	wg.Add(1)
	go scrapeMoviePopular(wg, csm, tmdbAPI, db, pf)

	wg.Add(1)
	go scrapeMovieDiscover(wg, csm, tmdbAPI, db, pf)

	wg.Add(1)
	go scrapeMovieTopRated(wg, csm, tmdbAPI, db, pf)

	wg.Add(1)
	go scrapeTvPopular(wg, cst, tmdbAPI, db, pf)

	wg.Add(1)
	go scrapeTvDiscover(wg, cst, tmdbAPI, db, pf)

	wg.Add(1)
	go scrapeTvTopRated(wg, csm, tmdbAPI, db, pf)

	wg.Wait()

	log.Info("scrape finished")

	return nil
}

func Start(tmdbAPI *tmdb.TMDb, db *mongo.Database, pf *poster.Fetcher) (context.Context, context.CancelFunc) {
	scrapeInterval := time.Minute * time.Duration(config.C.Rates.ScrapeInterval)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		if err := runScrape(tmdbAPI, db, pf); err != nil {
			log.Error(err)
		}

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
