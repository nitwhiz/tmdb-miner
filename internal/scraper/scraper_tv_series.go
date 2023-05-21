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

type baseTvGetterFunc func(api *tmdb.TMDb, opts map[string]string) (*tmdb.TvPagedResults, error)

func GetTvDiscover(api *tmdb.TMDb, opts map[string]string) (*tmdb.TvPagedResults, error) {
	return api.DiscoverTV(opts)
}

func GetTvPopular(api *tmdb.TMDb, opts map[string]string) (*tmdb.TvPagedResults, error) {
	return api.GetTvPopular(opts)
}

func GetTvTopRated(api *tmdb.TMDb, opts map[string]string) (*tmdb.TvPagedResults, error) {
	return api.GetTvTopRated(opts)
}

func GetChangedTvIds(api *tmdb.TMDb) (map[int]struct{}, error) {
	result := map[int]struct{}{}

	page := 1

	for {
		log.WithFields(log.Fields{
			"type": "tv_series",
			"page": page,
		}).Info("fetching changes page")

		changes, err := api.GetChangesTv(map[string]string{
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

func FetchTvSeries(baseGetter baseTvGetterFunc, cs map[int]struct{}, api *tmdb.TMDb, db *mongo.Database, pf *poster.Fetcher, maxPages int) error {
	l := log.WithFields(log.Fields{
		"type": "tv_series",
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

		tv, err := baseGetter(api, opts)

		requestCounter += 1

		if err != nil {
			return err
		}

		for _, t := range tv.Results {
			l = l.WithFields(
				log.Fields{
					"id": t.ID,
				},
			)

			_, changed := cs[t.ID]

			if changed {
				log.Info("changed")
			} else {
				log.Info("not changed")
			}

			err := db.Collection(CollectionTvShort).FindOne(
				context.TODO(),
				bson.D{{"id", tv.ID}},
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

			l.Infof("persisting short `%s`\n", t.Name)

			_, err = db.Collection(CollectionTvShort).UpdateOne(
				context.TODO(),
				bson.D{{"id", t.ID}},
				bson.D{{"$set", t}},
				options.Update().SetUpsert(true),
			)

			if err != nil {
				return err
			}

			time.Sleep(10 * time.Millisecond)

			ti, err := api.GetTvInfo(t.ID, map[string]string{
				"language": Language,
			})

			requestCounter += 1

			if err != nil {
				l.Error(err)
				continue
			}

			l.Infof("persisting detail `%s`\n", t.Name)

			_, err = db.Collection(CollectionTvDetail).UpdateOne(
				context.TODO(),
				bson.D{{"id", ti.ID}},
				bson.D{{"$set", ti}},
				options.Update().SetUpsert(true),
			)

			if err != nil {
				panic(err)
			}

			if ti.PosterPath != "" {
				l.Info("persisting poster")

				time.Sleep(10 * time.Millisecond)

				if err := pf.Download(ti.PosterPath, "tv-"+strconv.Itoa(ti.ID)); err != nil {
					log.Error("error persisting poster", err)
				}
			}
		}

		page = tv.Page + 1
		totalPages = tv.TotalPages

		opts["page"] = strconv.Itoa(page)
	}

	return nil
}

func scrapeTvPopular(wg *sync.WaitGroup, cs map[int]struct{}, tmdbAPI *tmdb.TMDb, db *mongo.Database, pf *poster.Fetcher) {
	defer wg.Done()

	if err := FetchTvSeries(GetTvPopular, cs, tmdbAPI, db, pf, config.C.Rates.PagesPerScrape); err != nil {
		log.Error(err)
	}
}

func scrapeTvDiscover(wg *sync.WaitGroup, cs map[int]struct{}, tmdbAPI *tmdb.TMDb, db *mongo.Database, pf *poster.Fetcher) {
	defer wg.Done()

	if err := FetchTvSeries(GetTvDiscover, cs, tmdbAPI, db, pf, config.C.Rates.PagesPerScrape); err != nil {
		log.Error(err)
	}
}

func scrapeTvTopRated(wg *sync.WaitGroup, cs map[int]struct{}, tmdbAPI *tmdb.TMDb, db *mongo.Database, pf *poster.Fetcher) {
	defer wg.Done()

	if err := FetchTvSeries(GetTvTopRated, cs, tmdbAPI, db, pf, config.C.Rates.PagesPerScrape); err != nil {
		log.Error(err)
	}
}
