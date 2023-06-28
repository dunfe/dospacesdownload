package main

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/joho/godotenv"
)

func main() {

	err := godotenv.Load(".env")
	if err != nil {
		fmt.Printf("Error loading environment variables file")
		os.Exit(1)
	}

	// Initialize the spaces client
	key := os.Getenv("SPACES_KEY")
	secret := os.Getenv("SPACES_SECRET")
	// this is the region in the spaces endpoint url
	spacesRegion := os.Getenv("SPACES_REGION")
	newSession, err := session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials(key, secret, ""),
		Endpoint:    aws.String(fmt.Sprintf("https://%s.digitaloceanspaces.com", spacesRegion)),
		// you weirdly have to specify this when using digital ocean spaces
		// doesn't matter what region you are actually in, US centric much :/
		Region:           aws.String("sgp1"),
		S3ForcePathStyle: aws.Bool(false),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing AWS client: %v\n", err)
		os.Exit(1)
	}
	s3Client := s3.New(newSession)

	// Get all objects in my spaces bucket
	input := &s3.ListObjectsInput{
		Bucket: aws.String("space-dmovie"),
	}
	objects, err := s3Client.ListObjects(input)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error listing bucket contents: %v\n", err)
		os.Exit(1)
	}

	// Then loop through and only append the images I want to download
	// I know they all start with /media/high_res, hence selecting those
	filePaths := []string{}
	for _, obj := range objects.Contents {
		fileName := aws.StringValue(obj.Key)
		filePaths = append(filePaths, fileName)
	}

	fmt.Printf("Found %d files to download\n", len(filePaths))

	// Now for all those files, loop through and download each one
	var waitGroup sync.WaitGroup
	for _, filePath := range filePaths {
		waitGroup.Add(1)
		go func(filePath string) {
			imageData := &s3.GetObjectInput{
				Bucket: aws.String("space-dmovie"),
				Key:    aws.String(filePath),
			}

			result, err := s3Client.GetObject(imageData)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error downloading file: %v\n", err)
			}
			// create local file name in D disk
			localFileName := "D:/Dmovie/spaces/" + filePath
			out, err := os.Create(localFileName)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error creating new image file: %v\n", err)
			}
			// defer out.Close()
			defer closeFile(out)

			_, err = io.Copy(out, result.Body)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error writing data to file: %v\n", err)
			}
			waitGroup.Done()
		}(filePath)
	}

	waitGroup.Wait()
	fmt.Println("Success! All files downloaded :-)")
}

func closeFile(f *os.File) {
	err := f.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error closing file: %v\n", err)
		os.Exit(1)
	}
}
