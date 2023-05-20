package scraper

import (
	"context"
	"github.com/nitwhiz/tmdb-scraper/internal/poster"
	"github.com/ryanbradynd05/go-tmdb"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strconv"
	"time"
)

func FetchTvSeries(api *tmdb.TMDb, db *mongo.Database, pf *poster.Fetcher, maxPages int) error {
	l := log.WithFields(log.Fields{
		"type": "tv_series",
	})

	totalPages := 2
	page := 1

	opts := map[string]string{
		"sort_by":  "rating.desc",
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

		tv, err := api.DiscoverTV(opts)

		if err != nil {
			return err
		}

		for _, t := range tv.Results {
			l = l.WithFields(
				log.Fields{
					"id": t.ID,
				},
			)

			l.Infof("persisting short `%s`\n", t.Name)

			_, err = db.Collection(CollectionTvShort).UpdateOne(
				context.TODO(),
				bson.D{{"id", t.ID}},
				bson.D{{"$set", t}},
				options.Update().SetUpsert(true),
			)

			if err != nil {
				panic(err)
			}

			ti, err := api.GetTvInfo(t.ID, map[string]string{
				"language": Language,
			})

			if err != nil {
				l.Warn(err)

				time.Sleep(50 * time.Millisecond)
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

				if err := pf.Download(ti.PosterPath, "tv-"+strconv.Itoa(ti.ID)); err != nil {
					log.Warn("error persisting poster", err)
				}
			}

			time.Sleep(50 * time.Millisecond)
		}

		page = tv.Page + 1
		totalPages = tv.TotalPages

		opts["page"] = strconv.Itoa(page)
	}

	return nil
}
