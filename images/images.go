package images

import (
	"bytes"
	"encoding/base64"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws/credentials"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/rwcarlsen/goexif/exif"
	log "github.com/sirupsen/logrus"
	str "gitlab.com/battler/modules/strings"
)

type uploadedPhoto struct {
	Path string
	File string
}

const (
	S3_BUCKET            = "csx-docs"
	S3_BUCKET_THUMBNAILS = "csx-docs-tn"
	S3_REGION            = "nl-ams"
	S3_API_ACCESS_KEY    = "SCWPT293A2FEE4NJZEW2"
	S3_API_SECRET_KEY    = "3c90bc78-e7d2-4b7a-a815-d64e3eaf7220"
	S3_API_TOKEN         = "79bb5344-f2e9-4aa3-9c7a-9ad2f41ef9e5"
)

func UploadImage(photo *string, dir *string) (*uploadedPhoto, error) {
	dec, err := base64.StdEncoding.DecodeString(*photo)
	if err != nil {
		log.Error("Error decode photo: ", err)
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
		log.Error("Error create file for photo: "+*dir+"/"+path, err)
		return nil, err
	}
	defer f.Close()

	if _, err := f.Write(dec); err != nil {
		log.Error("Error write file for photo: "+*dir+"/"+path, err)
		return nil, err
	}
	if err := f.Sync(); err != nil {
		log.Error("Error commit content file for photo: "+*dir+"/"+path, err)
		return nil, err
	}

	res := uploadedPhoto{
		Path: path,
		File: file + ".jpg"}

	return &res, nil
}

func UploadImageS3(photo *string, dir *string) (*uploadedPhoto, error) {
	dec, err := base64.StdEncoding.DecodeString(*photo)
	if err != nil {
		log.Error("Error decode photo: ", err)
		return nil, err
	}
	var x *exif.Exif
	x, err = exif.Decode(bytes.NewBuffer(dec))
	if err != nil {
		log.Error("Error decode exif: ", err)
		return nil, err
	}
	var thumbnail []byte
	thumbnail, err = x.JpegThumbnail()
	if err != nil {
		log.Error("Error gen thumbnail: ", err)
		return nil, err
	}

	file := str.RandomString(10, false)
	dest := strings.Join(strings.Split(file, ""), "/")
	path := dest + "/" + file + ".jpg"

	s, err := session.NewSession(&aws.Config{
		Region:      aws.String(S3_REGION),
		Endpoint:    aws.String("https://s3.nl-ams.scw.cloud"),
		Credentials: credentials.NewStaticCredentials(S3_API_ACCESS_KEY, S3_API_SECRET_KEY, S3_API_TOKEN),
	})
	if err != nil {
		log.Error("Error create s3 session", err)
		return nil, err
	}

	_, err = s3.New(s).PutObject(&s3.PutObjectInput{
		Bucket:             aws.String(S3_BUCKET),
		Key:                aws.String(path),
		ACL:                aws.String("private"),
		Body:               bytes.NewReader(dec),
		ContentLength:      aws.Int64(int64(len(dec))),
		ContentType:        aws.String(http.DetectContentType(dec)),
		ContentDisposition: aws.String("attachment"),
	})
	if err != nil {
		log.Error("Error upload file to s3", err)
		return nil, err
	}

	if len(thumbnail) > 0 {
		_, err = s3.New(s).PutObject(&s3.PutObjectInput{
			Bucket:             aws.String(S3_BUCKET_THUMBNAILS),
			Key:                aws.String(path),
			ACL:                aws.String("private"),
			Body:               bytes.NewReader(thumbnail),
			ContentLength:      aws.Int64(int64(len(thumbnail))),
			ContentType:        aws.String(http.DetectContentType(thumbnail)),
			ContentDisposition: aws.String("attachment"),
		})
		if err != nil {
			log.Error("Error upload file to s3", err)
			return nil, err
		}
	}

	res := uploadedPhoto{
		Path: path,
		File: file + ".jpg"}

	return &res, nil
}

func GetImageS3(path string) (*[]byte, error) {
	s, err := session.NewSession(&aws.Config{
		Region:      aws.String(S3_REGION),
		Endpoint:    aws.String("https://s3.nl-ams.scw.cloud"),
		Credentials: credentials.NewStaticCredentials(S3_API_ACCESS_KEY, S3_API_SECRET_KEY, S3_API_TOKEN),
	})
	if err != nil {
		log.Error("Error create s3 session", err)
		return nil, err
	}

	res, err := s3.New(s).GetObject(&s3.GetObjectInput{
		Bucket: aws.String(S3_BUCKET),
		Key:    aws.String(path),
	})
	if err != nil {
		log.Error(err)
		return nil, err
	}
	image, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	if err != nil {
		log.Error("Error upload file to s3", err)
		return nil, err
	}
	return &image, nil
}
