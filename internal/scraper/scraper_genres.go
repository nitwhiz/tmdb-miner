package scraper

import (
	"context"
	"github.com/ryanbradynd05/go-tmdb"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func FetchMovieGenres(api *tmdb.TMDb, db *mongo.Database) error {
	l := log.WithFields(log.Fields{
		"type": "movie_genres",
	})

	l.Info("fetching genres")

	genres, err := api.GetMovieGenres(map[string]string{
		"language": Language,
	})

	if err != nil {
		return err
	}

	for _, g := range genres.Genres {
		l = l.WithFields(
			log.Fields{
				"id": g.ID,
			},
		)

		l.Infof("persisting genre `%s`\n", g.Name)

		_, err = db.Collection(CollectionMovieCategory).UpdateOne(
			context.TODO(),
			bson.D{{"id", g.ID}},
			bson.D{{"$set", g}},
			options.Update().SetUpsert(true),
		)

		if err != nil {
			panic(err)
		}
	}

	return nil
}

func FetchTvSeriesGenres(api *tmdb.TMDb, db *mongo.Database) error {
	l := log.WithFields(log.Fields{
		"type": "tv_series_genres",
	})

	l.Info("fetching genres")

	genres, err := api.GetTvGenres(map[string]string{
		"language": Language,
	})

	if err != nil {
		return err
	}

	for _, g := range genres.Genres {
		l = l.WithFields(
			log.Fields{
				"id": g.ID,
			},
		)

		l.Infof("persisting genre `%s`\n", g.Name)

		_, err = db.Collection(CollectionTvCategory).UpdateOne(
			context.TODO(),
			bson.D{{"id", g.ID}},
			bson.D{{"$set", g}},
			options.Update().SetUpsert(true),
		)

		if err != nil {
			panic(err)
		}
	}

	return nil
}
