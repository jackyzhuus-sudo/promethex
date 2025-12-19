package base

import (
	"bytes"
	"context"
	"io"
	"market-service/internal/conf"
	"market-service/internal/pkg/common"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-kratos/kratos/v2/log"
)

type S3Client struct {
	BucketBiz   string
	BucketAdmin string
	Region      string
	s3Cli       *s3.Client
}

func newS3Client(c *conf.Data) *S3Client {
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(c.S3.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			c.S3.AccessKey,
			c.S3.SecretKey,
			"",
		)),
	)

	if err != nil {
		log.Fatal(err)
	}
	cli := &S3Client{
		BucketBiz:   c.S3.BizBucket,
		BucketAdmin: c.S3.AdminBucket,
		Region:      c.S3.Region,
		s3Cli:       s3.NewFromConfig(cfg),
	}

	// TODO check s3 client init success or not
	return cli
}

func (r Infra) UploadFileToBizBucketS3(ctx common.Ctx, fileData []byte, key string) error {
	_, err := r.s3.s3Cli.PutObject(ctx.Ctx, &s3.PutObjectInput{
		Bucket: aws.String(r.s3.BucketBiz),
		Key:    aws.String(key),
		Body:   bytes.NewReader(fileData),
	})
	if err != nil {
		return err
	}
	return nil
}

func (r Infra) UploadFileToAdminBucketS3(ctx common.Ctx, fileData []byte, key string) error {
	_, err := r.s3.s3Cli.PutObject(ctx.Ctx, &s3.PutObjectInput{
		Bucket: aws.String(r.s3.BucketAdmin),
		Key:    aws.String(key),
		Body:   bytes.NewReader(fileData),
	})
	if err != nil {
		return err
	}
	return nil
}

func (r Infra) DownloadFileFromBizBucketS3(ctx common.Ctx, key string) ([]byte, string, error) {
	rsp, err := r.s3.s3Cli.GetObject(ctx.Ctx, &s3.GetObjectInput{
		Bucket: aws.String(r.s3.BucketBiz),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, "", err
	}
	defer rsp.Body.Close()

	body, err := io.ReadAll(rsp.Body)
	if err != nil {
		return nil, "", err
	}

	return body, *rsp.ContentType, nil
}

func (r Infra) DownloadFileFromAdminBucketS3(ctx common.Ctx, key string) ([]byte, string, error) {
	rsp, err := r.s3.s3Cli.GetObject(ctx.Ctx, &s3.GetObjectInput{
		Bucket: aws.String(r.s3.BucketAdmin),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, "", err
	}
	defer rsp.Body.Close()
	body, err := io.ReadAll(rsp.Body)
	if err != nil {
		return nil, "", err
	}
	return body, *rsp.ContentType, nil
}
