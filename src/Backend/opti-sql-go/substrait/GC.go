package substrait

import (
	"context"
	"fmt"
	"opti-sql-go/config"
	"time"

	"github.com/minio/minio-go"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// garbage collection for removing files from s3 storage after expiration
var dontTouchTestFiles = []string{"country_full.csv", "userdata.parquet", "example.txt", "random_test"}

const ignoreFolder = "result-file-cache"
const loggerPrefix = "Garbage-Collection"
const waitTime = time.Second * 5

func garbageCollection() {
	logger := config.GetLogger()
	logger.Info(fmt.Sprintf("[%v]starting garbage collection, won't touch these files %v", loggerPrefix, dontTouchTestFiles))
	config := config.GetConfig()
	redisInstance := redis.NewClient(&redis.Options{
		Addr:     config.Server.RedisAddr + ":6379",
		Password: "", // no password
		DB:       0,  // use default DB
		Protocol: 2,
	})
	secretes := config.Secretes
	accessKey := secretes.AccessKey
	secretKey := secretes.SecretKey
	endpoint := secretes.EndpointURL
	bucket := secretes.BucketName
	useSSL := true

	client, err := minio.New(endpoint, accessKey, secretKey, useSSL)
	if err != nil {
		logger.Fatal("failed to construct s3 client to delete old files", zap.String("error message", fmt.Sprintf("%v", err)))
	}
	var failedAttempts = 0
	for {
	start:
		if failedAttempts > 5 {
			logger.Warn("removing files has failed over 5 times, check redis and s3 for issues !!!")
		}
		fmt.Printf("waiting %v minutes before check for files to clear from s3", waitTime.Minutes())
		time.Sleep(waitTime)
		start := time.Now()
		entries, err := redisInstance.LRange(context.TODO(), ignoreFolder, 0, -1).Result()
		if err != nil {
			logger.Error(fmt.Sprintf("failed to read in files from %v", ignoreFolder), zap.Int("fail counter", failedAttempts))
			failedAttempts++
			goto start // try again
		}
		// read all the files in s3
		doneChan := make(chan struct{})
		readCount := 0
		var nonValidFiles []string
		validMap := buildMap(dontTouchTestFiles, entries)
		for fileName := range client.ListObjects(bucket, "", true, doneChan) {
			if !validMap[fileName.Key] {
				nonValidFiles = append(nonValidFiles, fileName.Key)
			}
			readCount++
		}
		var removedFiles = 0
		for _, invalidFile := range nonValidFiles {
			err := client.RemoveObject(bucket, invalidFile)
			if err != nil {
				logger.Warn(fmt.Sprintf("error removing %v from s3: %v", invalidFile, err))
				// log and move on
			} else {
				removedFiles++
			}
		}
		failedAttempts = 0 // reset failed attempts back to zero
		logger.Info("Garbage Collection metrics", zap.Any("to-keep map", validMap), zap.Int("total-files count", readCount), zap.Int("removed-files count", removedFiles), zap.Any("time-taken", time.Since(start)))

	}

}
func buildMap(source1 []string, source2 []string) map[string]bool {
	result := make(map[string]bool)
	for _, k := range source1 {
		result[k] = true
	}
	for _, k := range source2 {
		result[k] = true
	}
	return result
}
