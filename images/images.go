package images

import (
	"encoding/base64"
	"os"
	"strings"

	"github.com/kataras/golog"
	str "gitlab.com/battler/modules/strings"
)

type uploadedPhoto struct {
	Path string
	File string
}

func UploadImage(photo *string, dir *string) (*uploadedPhoto, error) {
	dec, err := base64.StdEncoding.DecodeString(*photo)
	if err != nil {
		golog.Error("Error decode photo: ")
		return nil, err
	}
	file := str.RandomString(10, false)
	dest := strings.Join(strings.Split(file, ""), "/")
	path := dest + "/" + file + ".jpg"

	if _, err := os.Stat(*dir + "/" + path); os.IsNotExist(err) {
		os.MkdirAll(*dir+"/"+dest, 0755)
	}
	f, err := os.Create(*dir + "/" + path)
	if err != nil {
		golog.Error("Error create file for photo: " + *dir + "/" + path)
		return nil, err
	}
	defer f.Close()

	if _, err := f.Write(dec); err != nil {
		golog.Error("Error write file for photo: " + *dir + "/" + path)
		return nil, err
	}
	if err := f.Sync(); err != nil {
		golog.Error("Error commit content file for photo: " + *dir + "/" + path)
		return nil, err
	}

	res := uploadedPhoto{
		Path: path,
		File: file + ".jpg"}

	return &res, nil
}
