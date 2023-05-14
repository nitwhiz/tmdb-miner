package main

import (
	"context"
	"github.com/ryanbradynd05/go-tmdb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"strconv"
	"time"
)

func main() {
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
		APIKey:   "TOP_SECRET",
		Proxies:  nil,
		UseProxy: false,
	}

	tmdbAPI = tmdb.Init(config)

	genres, err := tmdbAPI.GetMovieGenres(map[string]string{
		"language": "de",
	})

	if err != nil {
		panic(err)
	}

	for _, g := range genres.Genres {
		collection := client.Database("tmdb").Collection("genres")

		_, err = collection.UpdateOne(context.TODO(), bson.D{{"id", g.ID}}, bson.D{{"$set", g}}, options.Update().SetUpsert(true))

		if err != nil {
			panic(err)
		}
	}

	movies, err := tmdbAPI.DiscoverMovie(map[string]string{
		"sort_by":  "rating.desc",
		"language": "de",
		"region":   "de-DE",
	})

	if err != nil {
		panic(err)
	}

	for movies.Page < movies.TotalPages {
		log.Printf("processing page %d/%d ...\n", movies.Page, movies.TotalPages)

		for _, m := range movies.Results {
			collection := client.Database("tmdb").Collection("movies_short")

			_, err = collection.UpdateOne(context.TODO(), bson.D{{"id", m.ID}}, bson.D{{"$set", m}}, options.Update().SetUpsert(true))

			if err != nil {
				panic(err)
			}

			mi, err := tmdbAPI.GetMovieInfo(m.ID, map[string]string{
				"language": "de",
			})

			if err != nil {
				log.Println(err)
			}

			collection = client.Database("tmdb").Collection("movies_detail")

			_, err = collection.UpdateOne(context.TODO(), bson.D{{"id", mi.ID}}, bson.D{{"$set", mi}}, options.Update().SetUpsert(true))

			if err != nil {
				panic(err)
			}
		}

		movies, err = tmdbAPI.DiscoverMovie(map[string]string{
			"sort_by":  "rating.desc",
			"language": "de",
			"region":   "de-DE",
			"page":     strconv.Itoa(movies.Page + 1),
		})

		if err != nil {
			panic(err)
		}

		time.Sleep(time.Millisecond * 100)
	}
}
