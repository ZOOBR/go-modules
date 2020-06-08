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
	"github.com/disintegration/imaging"
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
		"csx-photo-damages-051019": "fr-par",
		"csx-photo-damages":        "fr-par",
	}
)

const (
	S3_CLIENT_DOCS_04102019 = "csx-docs041019"
	// S3_CLIENT_DOCS          = "csx-docs"
	S3_FINES = "csx-fines"
	// S3_OBJECT_DAMAGES = "csx-photo-damages"
	S3_PUBLIC         = "csx-public"
	S3_RENT_PHOTO     = "csx-rent-photo-200608"
	S3_RENT_PHOTO_TH  = "csx-rent-photo-th-200608"
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
	"public":              S3_PUBLIC,
	"selfie":              S3_SELFIE,
	"rentPhoto":           S3_RENT_PHOTO,
	"rentPhotoThumbnails": S3_RENT_PHOTO_TH,
}

var bucketsMultiple = map[string]map[string]string{
	"docs":    S3_CLIENT_DOCS,
	"damages": S3_OBJECT_DAMAGES,
}

var regionsMap = map[string]string{
	"docs":                "nl-ams",
	"docs-04102019":       "fr-par",
	"fines":               "fr-par",
	"damages":             "fr-par",
	"public":              "nl-ams",
	"selfie":              "fr-par",
	"rentPhoto":           "nl-ams",
	"rentPhotoThumbnails": "nl-ams",
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

// MakeThumbnail makes photo thumbnail from exif or without
func MakeThumbnail(data []byte, x *exif.Exif, thumbSize int64) (*[]byte, error) {
	img, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		log.Error("Error gen thumbnail: ", err)
		return nil, err
	}
	var xSize, ySize uint32
	var orientation uint16
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
		orientationTag, err := x.Get("Orientation")
		if err != nil {
			log.Error("Error gen orientation: ", err)
		}
		xSize = binary.BigEndian.Uint32(xTag.Val)
		ySize = binary.BigEndian.Uint32(yTag.Val)
		if orientationTag != nil {
			orientation = binary.BigEndian.Uint16(orientationTag.Val)
		}

	}

	var ratio float32 = float32(xSize) / float32(ySize)
	var thX, thY uint
	if ratio > 1 {
		thX = uint(thumbSize)
		thY = 0
	} else {
		thY = uint(thumbSize)
		thX = 0
	}
	dstThumb := resize.Resize(thX, thY, img, resize.Lanczos3)
	if orientation > 0 {
		switch orientation {
		case 6:
			dst := imaging.Rotate270(dstThumb)
			dstThumb = image.Image(dst)
			break
		case 3:
			dst := imaging.Rotate180(dstThumb)
			dstThumb = image.Image(dst)
			break
		case 8:
			dst := imaging.Rotate90(dstThumb)
			dstThumb = image.Image(dst)
			break
		}
	}
	w := bytes.NewBuffer([]byte{})
	err = jpeg.Encode(w, dstThumb, nil)
	if err != nil {
		log.Error("Error gen thumbnail: ", err)
		return nil, err
	}
	res := w.Bytes()
	return &res, nil
}

// UploadFileS3 loads file to S3 storage
func UploadFileS3(bucketName, acl, filename string, rawData multipart.File) error {
	data, err := ioutil.ReadAll(rawData)
	if err != nil {
		return errors.New("Error file read: " + err.Error())
	}

	regionS3, ok := regionsMap[bucketName]
	if !ok {
		return errors.New("Region not found")
	}

	ses, err := session.NewSession(&aws.Config{
		Region:      aws.String(regionS3),
		Endpoint:    aws.String("https://s3." + regionS3 + ".scw.cloud"),
		Credentials: credentials.NewStaticCredentials(S3_API_ACCESS_KEY, S3_API_SECRET_KEY, S3_API_TOKEN),
	})
	if err != nil {
		return errors.New("Error create s3 session: " + err.Error())
	}

	bucket, ok := bucketsMap[bucketName]
	if !ok {
		return errors.New("Bucket not found")
	}

	_, err = s3.New(ses).PutObject(&s3.PutObjectInput{
		Bucket:             aws.String(bucket),
		Key:                aws.String(filename),
		ACL:                aws.String(acl),
		Body:               bytes.NewReader(data),
		ContentLength:      aws.Int64(int64(len(data))),
		ContentType:        aws.String(http.DetectContentType(data)),
		ContentDisposition: aws.String("attachment"),
	})
	if err != nil {
		return errors.New("Error upload file to s3" + err.Error())
	}
	return nil
}

// UploadImageS3 loads image to S3 storage with thumbnail
func UploadImageS3(photo *string, bucketName string, existPath *string, thumbSize int64, rawData ...multipart.File) (*uploadedPhoto, error) {
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
		newThumbnail, err := MakeThumbnail(dec, x, thumbSize)
		if err != nil {
			return nil, err
		} else if newThumbnail == nil {
			return nil, errors.New("Empty thumbnail")
		}
		thumbnail = *newThumbnail
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
		savedBucketName = &bucket
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

// UploadImageS3CustomThumb loads image to S3 storage with thumbnail to custom thumb bucket
func UploadImageS3CustomThumb(photo *string, bucketName string, existPath *string, thumbSize int64, rawData multipart.File, thBucket string) (*uploadedPhoto, error) {
	var r io.Reader
	if rawData == nil && photo != nil {
		var err error
		dec, err := base64.StdEncoding.DecodeString(*photo)
		if err != nil {
			log.Error("Error decode photo: ", err)
			return nil, err
		}
		r = bytes.NewReader(dec)
	} else {
		r = rawData
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
		newThumbnail, err := MakeThumbnail(dec, x, thumbSize)
		if err != nil {
			return nil, err
		} else if newThumbnail == nil {
			return nil, errors.New("Empty thumbnail")
		}
		thumbnail = *newThumbnail
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
		savedBucketName = &bucket
	}
	var thBucketS3Name *string
	if bucketName != "fines" {
		thBucketS3Name, err = UploadThumbnail(thumbnail, path, thBucket)
		if err != nil {
			log.Error("Error upload thumbnail: ", err)
			return nil, err
		}
	}

	res := uploadedPhoto{
		Path:            path,
		Bucket:          savedBucketName,
		ThumbnailBucket: thBucketS3Name,
		File:            file + ".jpg"}

	return &res, nil
}

func UploadThumbnail(thumbnail []byte, path string, thBucket ...string) (*string, error) {
	var err error
	if len(thumbnail) == 0 {
		return nil, errors.New("empty thumbnail")
	}
	if len(thBucket) > 0 && thBucket[0] != "" {
		thBucketName := thBucket[0]
		bucket, ok := bucketsMap[thBucketName]
		if !ok {
			return nil, errors.New("invalid thumbnail bucket: " + thBucketName)
		}
		region, ok := regionsMap[thBucketName]
		if !ok {
			return nil, errors.New("invalid thumbnail bucket region: " + thBucketName)
		}
		err := apiS3Put(bucket, region, path, thumbnail)
		if err != nil {
			log.Error("Error upload thumbnail to: ", bucket)
		} else {
			return &bucket, nil
		}
	} else {
		for bucket, region := range S3_BUCKET_THUMBNAILS {
			err := apiS3Put(bucket, region, path, thumbnail)
			if err != nil {
				log.Error("Error upload thumbnail to: ", bucket)
			} else {
				return &bucket, nil
			}
		}
	}
	return nil, err
}

func apiS3Put(bucket, region, path string, data []byte) error {
	th, err := session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Endpoint:    aws.String("https://s3." + region + ".scw.cloud"),
		Credentials: credentials.NewStaticCredentials(S3_API_ACCESS_KEY, S3_API_SECRET_KEY, S3_API_TOKEN),
	})
	if err != nil {
		return err
	}
	_, err = s3.New(th).PutObject(&s3.PutObjectInput{
		Bucket:             aws.String(bucket),
		Key:                aws.String(path),
		ACL:                aws.String("private"),
		Body:               bytes.NewReader(data),
		ContentLength:      aws.Int64(int64(len(data))),
		ContentType:        aws.String(http.DetectContentType(data)),
		ContentDisposition: aws.String("attachment"),
	})
	return err
}

func GetImageS3(path string, bucketName string, isThumbnail bool, thBucket *string) (*[]byte, error) {
	var imageBody io.ReadCloser
	if bucketName == "docs" || bucketName == "damages" {
		buckets, ok := bucketsMultiple[bucketName]
		if !ok {
			return nil, errors.New("Bucket not found")
		}
		imgParts := strings.Split(path, "&")
		if len(imgParts) > 1 && !isThumbnail {
			b := imgParts[1]
			region, ok := buckets[b]
			if !ok {
				return nil, errors.New("Region not found")
			}
			var err error
			imageBody, err = getS3Object(imgParts[0], b, region)
			if err != nil {
				return nil, err
			}
		} else {
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
				img, err := getS3Object(path, bucket, region)
				if err != nil {
					log.Error(err)
				} else {
					imageBody = img
					break
				}
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
		var err error
		imageBody, err = getS3Object(path, bucket, regionS3)
		if err != nil {
			return nil, err
		}
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

func getS3Object(path, bucket, region string) (io.ReadCloser, error) {
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
		return nil, err
	}
	return res.Body, nil
}
