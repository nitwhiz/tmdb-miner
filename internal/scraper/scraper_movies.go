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

func FetchMovies(api *tmdb.TMDb, db *mongo.Database, pf *poster.Fetcher, maxPages int) error {
	l := log.WithFields(log.Fields{
		"type": "movies",
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

		movies, err := api.DiscoverMovie(opts)

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

			if err != nil {
				l.Warn(err)

				time.Sleep(50 * time.Millisecond)
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

			time.Sleep(50 * time.Millisecond)
		}

		page = movies.Page + 1
		totalPages = movies.TotalPages

		opts["page"] = strconv.Itoa(page)
	}

	return nil
}
