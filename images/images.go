package images

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"image"
	"image/jpeg"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws/credentials"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	//"github.com/rwcarlsen/goexif/exif"
	"github.com/xor-gate/goexif2/exif"

	"github.com/nfnt/resize"
	log "github.com/sirupsen/logrus"
	str "gitlab.com/battler/modules/strings"
)

type uploadedPhoto struct {
	Path            string
	Bucket          *string
	ThumbnailBucket *string
	File            string
}

var (
	S3_BUCKET_THUMBNAILS = map[string]string{
		"csx-docs-tn-051019": "fr-par",
		"csx-docs-tn":        "nl-ams",
	}
	S3_CLIENT_DOCS = map[string]string{
		"csx-docs041019": "fr-par",
		"csx-docs":       "nl-ams",
	}
	S3_OBJECT_DAMAGES = map[string]string{
		"csx-photo-damages":        "fr-par",
		"csx-photo-damages-051019": "fr-par",
	}
)

const (
	S3_CLIENT_DOCS_04102019 = "csx-docs041019"
	// S3_CLIENT_DOCS          = "csx-docs"
	S3_FINES = "csx-fines"
	// S3_OBJECT_DAMAGES = "csx-photo-damages"
	S3_PUBLIC         = "csx-public"
	S3_SELFIE         = "csx-selfie"
	S3_REGION         = "nl-ams"
	S3_API_ACCESS_KEY = "SCWPT293A2FEE4NJZEW2"
	S3_API_SECRET_KEY = "3c90bc78-e7d2-4b7a-a815-d64e3eaf7220"
	S3_API_TOKEN      = "79bb5344-f2e9-4aa3-9c7a-9ad2f41ef9e5"
)

var bucketsMap = map[string]string{
	// "docs":          S3_CLIENT_DOCS,
	"docs-04102019": S3_CLIENT_DOCS_04102019,
	"fines":         S3_FINES,
	// "damages":       S3_OBJECT_DAMAGES,
	"public": S3_PUBLIC,
	"selfie": S3_SELFIE,
}

var bucketsMultiple = map[string]map[string]string{
	"docs":    S3_CLIENT_DOCS,
	"damages": S3_OBJECT_DAMAGES,
}

var regionsMap = map[string]string{
	"docs":          "nl-ams",
	"docs-04102019": "fr-par",
	"fines":         "fr-par",
	"damages":       "fr-par",
	"public":        "nl-ams",
	"selfie":        "fr-par",
}

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

func MakeThumbnail(data []byte, x *exif.Exif) (*[]byte, error) {
	img, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		log.Error("Error gen thumbnail: ", err)
		return nil, err
	}
	var xSize, ySize uint32
	if x == nil {
		b := img.Bounds()
		xSize = uint32(b.Max.X)
		ySize = uint32(b.Max.Y)
	} else {
		xTag, err := x.Get("PixelXDimension")
		if err != nil {
			log.Error("Error gen thumbnail: ", err)
			return nil, err
		}
		yTag, err := x.Get("PixelYDimension")
		if err != nil {
			log.Error("Error gen thumbnail: ", err)
			return nil, err
		}
		xSize = binary.BigEndian.Uint32(xTag.Val)
		ySize = binary.BigEndian.Uint32(yTag.Val)
	}

	ratio := xSize / ySize
	var thX, thY uint
	if ratio > 1 {
		thX = 176
		// thY = int(float64(ySize) * (float64(thX) / float64(xSize)))
		thY = 0
	} else {
		thY = 176
		// thX = int(float64(xSize) * (float64(thY) / float64(ySize)))
		thX = 0
	}
	// dstThumb := imaging.Thumbnail(img, thX, thY, imaging.Lanczos)
	// thumbnail = dstThumb.Pix
	dstThumb := resize.Resize(thX, thY, img, resize.Lanczos3)
	w := bytes.NewBuffer([]byte{})
	err = jpeg.Encode(w, dstThumb, nil)
	if err != nil {
		log.Error("Error gen thumbnail: ", err)
		return nil, err
	}
	res := w.Bytes()
	return &res, nil
}

func UploadImageS3(photo *string, bucketName string, existPath *string, rawData ...multipart.File) (*uploadedPhoto, error) {
	var r io.Reader
	if len(rawData) == 0 || (len(rawData) > 0 && rawData[0] == nil) {
		var err error
		dec, err := base64.StdEncoding.DecodeString(*photo)
		if err != nil {
			log.Error("Error decode photo: ", err)
			return nil, err
		}
		r = bytes.NewReader(dec)
	} else {
		r = rawData[0]
	}
	dec, err := ioutil.ReadAll(r)
	if err != nil {
		log.Error("Error read buf: ", err)
		return nil, err
	}

	var thumbnail []byte
	if bucketName != "fines" {
		var x *exif.Exif
		x, err = exif.Decode(bytes.NewReader(dec))
		if err != nil && existPath == nil {
			log.Error("Error decode exif: ", err)
			return nil, err
		}
		if x != nil {
			thumbnail, err = x.JpegThumbnail()
		}
		if err != nil || len(thumbnail) == 0 {
			newThumbnail, err := MakeThumbnail(dec, x)
			if err != nil {
				return nil, err
			} else if newThumbnail == nil {
				return nil, errors.New("Empty thumbnail")
			}
			thumbnail = *newThumbnail
		}
	}

	file := str.RandomString(10, false)
	dest := strings.Join(strings.Split(file, ""), "/")
	path := dest + "/" + file + ".jpg"

	if existPath != nil {
		path = *existPath
	}
	var savedBucketName *string
	if bucketName == "docs" || bucketName == "damages" {
		buckets, ok := bucketsMultiple[bucketName]
		if !ok {
			return nil, errors.New("Bucket not found")
		}
		for bucket, region := range buckets {
			s, err := session.NewSession(&aws.Config{
				Region:      aws.String(region),
				Endpoint:    aws.String("https://s3." + region + ".scw.cloud"),
				Credentials: credentials.NewStaticCredentials(S3_API_ACCESS_KEY, S3_API_SECRET_KEY, S3_API_TOKEN),
			})
			if err != nil {
				log.Error("Error create s3 session", err)
				return nil, err
			}

			_, err = s3.New(s).PutObject(&s3.PutObjectInput{
				Bucket:             aws.String(bucket),
				Key:                aws.String(path),
				ACL:                aws.String("private"),
				Body:               bytes.NewReader(dec),
				ContentLength:      aws.Int64(int64(len(dec))),
				ContentType:        aws.String(http.DetectContentType(dec)),
				ContentDisposition: aws.String("attachment"),
			})
			if err != nil {
				log.Error("Error upload file to s3", err)
			} else {
				savedBucketName = &bucket
				break
			}
		}
	} else {
		regionS3, ok := regionsMap[bucketName]
		if !ok {
			return nil, errors.New("Region not found")
		}

		s, err := session.NewSession(&aws.Config{
			Region:      aws.String(regionS3),
			Endpoint:    aws.String("https://s3." + regionS3 + ".scw.cloud"),
			Credentials: credentials.NewStaticCredentials(S3_API_ACCESS_KEY, S3_API_SECRET_KEY, S3_API_TOKEN),
		})
		if err != nil {
			log.Error("Error create s3 session", err)
			return nil, err
		}

		bucket, ok := bucketsMap[bucketName]
		if !ok {
			return nil, errors.New("Bucket not found")
		}

		_, err = s3.New(s).PutObject(&s3.PutObjectInput{
			Bucket:             aws.String(bucket),
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
	}

	var thBucket *string
	if bucketName != "fines" {
		thBucket, err = UploadThumbnail(thumbnail, path)
		if err != nil {
			log.Error("Error upload thumbnail: ", err)
			return nil, err
		}
	}

	res := uploadedPhoto{
		Path:            path,
		Bucket:          savedBucketName,
		ThumbnailBucket: thBucket,
		File:            file + ".jpg"}

	return &res, nil
}

func UploadThumbnail(thumbnail []byte, path string) (*string, error) {
	var err error
	for bucket, region := range S3_BUCKET_THUMBNAILS {
		th, err := session.NewSession(&aws.Config{
			Region:      aws.String(region),
			Endpoint:    aws.String("https://s3." + region + ".scw.cloud"),
			Credentials: credentials.NewStaticCredentials(S3_API_ACCESS_KEY, S3_API_SECRET_KEY, S3_API_TOKEN),
		})
		if err != nil {
			return nil, err
		}
		if len(thumbnail) > 0 {
			_, err = s3.New(th).PutObject(&s3.PutObjectInput{
				Bucket:             aws.String(bucket),
				Key:                aws.String(path),
				ACL:                aws.String("private"),
				Body:               bytes.NewReader(thumbnail),
				ContentLength:      aws.Int64(int64(len(thumbnail))),
				ContentType:        aws.String(http.DetectContentType(thumbnail)),
				ContentDisposition: aws.String("attachment"),
			})
			if err != nil {
				log.Error("Error upload thumbnail to: ", bucket)
			} else {
				return &bucket, nil
			}
		}
	}
	return nil, err
}

func GetImageS3(path string, bucketName string, isThumbnail bool, thBucket *string) (*[]byte, error) {
	var imageBody io.ReadCloser
	if bucketName == "docs" || bucketName == "damages" {
		buckets, ok := bucketsMultiple[bucketName]
		if !ok {
			return nil, errors.New("Bucket not found")
		}
		for bucket, region := range buckets {
			if isThumbnail {
				parts := strings.Split(path, "&")
				if len(parts) > 1 {
					b := parts[1]
					path = parts[0]
					thBucket = &b
				}
				if thBucket != nil {
					bucket = *thBucket
					region = S3_BUCKET_THUMBNAILS[*thBucket]
				} else {
					bucket = "csx-docs-tn"
					region = "nl-ams"
				}
			}
			s, err := session.NewSession(&aws.Config{
				Region:      aws.String(region),
				Endpoint:    aws.String("https://s3." + region + ".scw.cloud"),
				Credentials: credentials.NewStaticCredentials(S3_API_ACCESS_KEY, S3_API_SECRET_KEY, S3_API_TOKEN),
			})
			if err != nil {
				log.Error("Error create s3 session", err)
				return nil, err
			}
			res, err := s3.New(s).GetObject(&s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(path),
			})
			if err != nil {
				log.Error(err)
			} else {
				imageBody = res.Body
				break
			}
		}
	} else {
		regionS3, ok := regionsMap[bucketName]
		if !ok {
			return nil, errors.New("Region not found")
		}
		bucket, ok := bucketsMap[bucketName]
		if !ok {
			return nil, errors.New("Bucket not found")
		}
		if isThumbnail {
			if thBucket != nil {
				bucket = *thBucket
				regionS3 = S3_BUCKET_THUMBNAILS[*thBucket]
			} else {
				bucket = "csx-docs-tn"
				regionS3 = "nl-ams"
			}
		}
		s, err := session.NewSession(&aws.Config{
			Region:      aws.String(regionS3),
			Endpoint:    aws.String("https://s3." + regionS3 + ".scw.cloud"),
			Credentials: credentials.NewStaticCredentials(S3_API_ACCESS_KEY, S3_API_SECRET_KEY, S3_API_TOKEN),
		})
		if err != nil {
			log.Error("Error create s3 session", err)
			return nil, err
		}
		res, err := s3.New(s).GetObject(&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(path),
		})
		if err != nil {
			log.Error(err)
			return nil, err
		}
		imageBody = res.Body
	}
	if imageBody == nil {
		return nil, errors.New("empty image body")
	}
	image, err := ioutil.ReadAll(imageBody)
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
