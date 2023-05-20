package scraper

import (
	"context"
	"github.com/nitwhiz/tmdb-scraper/internal/config"
	"github.com/nitwhiz/tmdb-scraper/internal/poster"
	"github.com/ryanbradynd05/go-tmdb"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strconv"
	"sync"
	"time"
)

type baseMovieGetterFunc func(api *tmdb.TMDb, opts map[string]string) (*tmdb.MoviePagedResults, error)

func GetMovieDiscover(api *tmdb.TMDb, opts map[string]string) (*tmdb.MoviePagedResults, error) {
	return api.DiscoverMovie(opts)
}

func GetMoviePopular(api *tmdb.TMDb, opts map[string]string) (*tmdb.MoviePagedResults, error) {
	return api.GetMoviePopular(opts)
}

func FetchMovies(baseGetter baseMovieGetterFunc, api *tmdb.TMDb, db *mongo.Database, pf *poster.Fetcher, maxPages int) error {
	l := log.WithFields(log.Fields{
		"type": "movies",
	})

	totalPages := 2
	page := 1

	opts := map[string]string{
		"language": Language,
		"region":   Region,
		"page":     strconv.Itoa(page),
	}

	for page < totalPages && page < maxPages {
		l = l.WithFields(log.Fields{
			"page":       opts["page"],
			"totalPages": totalPages,
		})

		l.Info("fetching page")

		movies, err := baseGetter(api, opts)

		requestCounter += 1

		if err != nil {
			return err
		}

		for _, m := range movies.Results {
			l = l.WithFields(
				log.Fields{
					"id": m.ID,
				},
			)

			l.Infof("persisting short `%s`\n", m.Title)

			_, err = db.Collection(CollectionMovieShort).UpdateOne(
				context.TODO(),
				bson.D{{"id", m.ID}},
				bson.D{{"$set", m}},
				options.Update().SetUpsert(true),
			)

			if err != nil {
				panic(err)
			}

			mi, err := api.GetMovieInfo(m.ID, map[string]string{
				"language": Language,
			})

			requestCounter += 1

			if err != nil {
				l.Warn(err)

				time.Sleep(10 * time.Millisecond)
				continue
			}

			l.Infof("persisting detail `%s`\n", m.Title)

			_, err = db.Collection(CollectionMovieDetail).UpdateOne(
				context.TODO(),
				bson.D{{"id", mi.ID}},
				bson.D{{"$set", mi}},
				options.Update().SetUpsert(true),
			)

			if err != nil {
				panic(err)
			}

			if mi.PosterPath != "" {
				l.Info("persisting poster")

				if err := pf.Download(mi.PosterPath, "movie-"+strconv.Itoa(mi.ID)); err != nil {
					log.Warn("error persisting poster", err)
				}
			}

			time.Sleep(10 * time.Millisecond)
		}

		page = movies.Page + 1
		totalPages = movies.TotalPages

		opts["page"] = strconv.Itoa(page)
	}

	return nil
}

func scrapeMoviePopular(wg *sync.WaitGroup, tmdbAPI *tmdb.TMDb, db *mongo.Database, pf *poster.Fetcher) {
	defer wg.Done()

	if err := FetchMovies(GetMoviePopular, tmdbAPI, db, pf, config.C.Rates.PagesPerScrape); err != nil {
		log.Error(err)
	}
}

func scrapeMovieDiscover(wg *sync.WaitGroup, tmdbAPI *tmdb.TMDb, db *mongo.Database, pf *poster.Fetcher) {
	defer wg.Done()

	if err := FetchMovies(GetMovieDiscover, tmdbAPI, db, pf, config.C.Rates.PagesPerScrape); err != nil {
		log.Error(err)
	}
}
