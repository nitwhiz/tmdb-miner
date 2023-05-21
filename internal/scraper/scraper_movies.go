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

func GetMovieTopRated(api *tmdb.TMDb, opts map[string]string) (*tmdb.MoviePagedResults, error) {
	return api.GetMovieTopRated(opts)
}

func GetChangedMovieIds(api *tmdb.TMDb) (map[int]struct{}, error) {
	result := map[int]struct{}{}

	page := 1

	for {
		log.WithFields(log.Fields{
			"type": "movies",
			"page": page,
		}).Info("fetching changes page")

		changes, err := api.GetChangesMovie(map[string]string{
			"page": strconv.Itoa(page),
		})

		requestCounter += 1

		if err != nil {
			return result, err
		}

		if len(changes.Results) == 0 {
			break
		}

		for _, c := range changes.Results {
			result[c.ID] = struct{}{}
		}

		page += 1

		time.Sleep(5 * time.Millisecond)
	}

	return result, nil
}

func FetchMovies(baseGetter baseMovieGetterFunc, cs map[int]struct{}, api *tmdb.TMDb, db *mongo.Database, pf *poster.Fetcher, maxPages int) error {
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

		time.Sleep(10 * time.Millisecond)

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

			_, changed := cs[m.ID]

			if changed {
				log.Info("changed")
			} else {
				log.Info("not changed")
			}

			err := db.Collection(CollectionMovieShort).FindOne(
				context.TODO(),
				bson.D{{"id", m.ID}},
			).Err()

			if err == mongo.ErrNoDocuments {
				log.Info("not indexed")
			} else if err == nil {
				log.Info("indexed")
			} else {
				log.Error(err)
				continue
			}

			if err == nil && !changed {
				l.Info("not changed, skipping")
				continue
			}

			l.Infof("persisting short `%s`\n", m.Title)

			_, err = db.Collection(CollectionMovieShort).UpdateOne(
				context.TODO(),
				bson.D{{"id", m.ID}},
				bson.D{{"$set", m}},
				options.Update().SetUpsert(true),
			)

			if err != nil {
				return err
			}

			time.Sleep(10 * time.Millisecond)

			mi, err := api.GetMovieInfo(m.ID, map[string]string{
				"language": Language,
			})

			requestCounter += 1

			if err != nil {
				l.Error(err)
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

				time.Sleep(10 * time.Millisecond)

				if err := pf.Download(mi.PosterPath, "movie-"+strconv.Itoa(mi.ID)); err != nil {
					log.Error("error persisting poster", err)
				}
			}
		}

		page = movies.Page + 1
		totalPages = movies.TotalPages

		opts["page"] = strconv.Itoa(page)
	}

	return nil
}

func scrapeMoviePopular(wg *sync.WaitGroup, cs map[int]struct{}, tmdbAPI *tmdb.TMDb, db *mongo.Database, pf *poster.Fetcher) {
	defer wg.Done()

	if err := FetchMovies(GetMoviePopular, cs, tmdbAPI, db, pf, config.C.Rates.PagesPerScrape); err != nil {
		log.Error(err)
	}
}

func scrapeMovieDiscover(wg *sync.WaitGroup, cs map[int]struct{}, tmdbAPI *tmdb.TMDb, db *mongo.Database, pf *poster.Fetcher) {
	defer wg.Done()

	if err := FetchMovies(GetMovieDiscover, cs, tmdbAPI, db, pf, config.C.Rates.PagesPerScrape); err != nil {
		log.Error(err)
	}
}

func scrapeMovieTopRated(wg *sync.WaitGroup, cs map[int]struct{}, tmdbAPI *tmdb.TMDb, db *mongo.Database, pf *poster.Fetcher) {
	defer wg.Done()

	if err := FetchMovies(GetMovieTopRated, cs, tmdbAPI, db, pf, config.C.Rates.PagesPerScrape); err != nil {
		log.Error(err)
	}
}
