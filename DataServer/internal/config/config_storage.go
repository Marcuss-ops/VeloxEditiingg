package config

import "os"

func loadStorageConfig() StorageConfig {
	c := StorageConfig{
		Region: "us-east-1",
	}
	c.Endpoint = os.Getenv("VELOX_S3_ENDPOINT")
	if r := os.Getenv("VELOX_S3_REGION"); r != "" {
		c.Region = r
	}
	c.Bucket = os.Getenv("VELOX_S3_BUCKET")
	c.AccessKeyID = os.Getenv("VELOX_S3_ACCESS_KEY_ID")
	c.SecretKey = os.Getenv("VELOX_S3_SECRET_ACCESS_KEY")
	c.UseSSL = boolFromEnv("VELOX_S3_USE_SSL", false)
	return c
}
