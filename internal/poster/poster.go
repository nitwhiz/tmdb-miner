package poster

import (
	"errors"
	"fmt"
	"github.com/nitwhiz/tmdb-scraper/internal/config"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
)

type Fetcher struct {
	baseUrl    string
	fsBasePath string
}

func NewFetcher() *Fetcher {
	return &Fetcher{
		baseUrl:    strings.TrimRight(config.C.Posters.BaseUrl, "/"),
		fsBasePath: strings.TrimRight(config.C.Posters.BaseDir, "/"),
	}
}

func (f *Fetcher) Download(srcPath string, mediaId string) error {
	if err := os.MkdirAll(f.fsBasePath, 0777); err != nil {
		return err
	}

	extension := path.Ext(srcPath)

	if extension == "" {
		return errors.New("missing extension")
	}

	posterUrl := fmt.Sprintf("%s/%s", f.baseUrl, strings.TrimLeft(srcPath, "/"))

	resp, err := http.Get(posterUrl)

	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return errors.New("poster response :" + strconv.Itoa(resp.StatusCode))
	}

	posterFilePath := fmt.Sprintf("%s/%s%s", f.fsBasePath, mediaId, extension)

	posterFile, err := os.Create(posterFilePath)

	if err != nil {
		return err
	}

	defer func(posterFile *os.File) {
		_ = posterFile.Close()
	}(posterFile)

	defer func(Body io.ReadCloser) {
		_ = Body.Close()
	}(resp.Body)

	_, err = io.Copy(posterFile, resp.Body)

	return err
}
