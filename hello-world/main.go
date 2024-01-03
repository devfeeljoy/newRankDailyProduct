package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
)

// API 응답 구조체
type ApiResponse struct {
	Msg  string     `json:"msg"`
	Code int        `json:"code"`
	Data []DataItem `json:"data"`
}

// 데이터 항목 구조체
type DataItem struct {
	URL  string `json:"url"`
	MD5  string `json:"md5"`
	Name string `json:"name"`
}

const MaxRetries = 5

func retryOperation(operation func() error) error {
	var lastErr error
	for i := 0; i < MaxRetries; i++ {
		err := operation()
		if err == nil {
			return nil
		}
		lastErr = err
		// 선택적으로 재시도 간 딜레이를 추가할 수 있습니다.
		time.Sleep(time.Second * 1)
	}
	return fmt.Errorf("operation failed after %d attempts: %v", MaxRetries, lastErr)
}

func downloadAvroFile(url string, filePath string) error {

	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// 파일 생성
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// 응답 본문을 파일에 쓰기
	_, err = io.Copy(file, resp.Body)
	return err
}

func uploadToS3(filePath string, bucketName string, s3Key string) error {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("ap-northeast-2"), // AWS 리전 설정
	})
	if err != nil {
		return err
	}

	s3Client := s3.New(sess)
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(s3Key),
		Body:   file,
	})
	if err != nil {
		return err
	}
	return err
}

// Lambda 핸들러 함수
func HandleRequest(ctx context.Context) (events.APIGatewayProxyResponse, error) {

	apiKey := os.Getenv("key") // 환경 변수에서 API 키 가져오기
	bucketName := os.Getenv("BUCKET_NAME")
	if apiKey == "" {
		fmt.Println("API key is not set in environment variables.")
		return events.APIGatewayProxyResponse{
			Body:       "API key is not set in environment variables.",
			StatusCode: 500,
		}, nil
	}

	// 전날 날짜 계산
	yesterday := time.Now().AddDate(0, 0, -1).Format("2006-01-02")

	apiUrl, err := url.Parse("https://api.newrank.cn/api/v2/custom/common/buridge/file/list")
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}
	// 쿼리 파라미터 추가
	query := apiUrl.Query()
	query.Set("date", yesterday)
	apiUrl.RawQuery = query.Encode()
	client := &http.Client{Timeout: 300 * time.Second}
	req, err := http.NewRequest("GET", apiUrl.String(), nil)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}

	// 헤더 설정
	req.Header.Set("key", apiKey)

	err = retryOperation(func() error {
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("API 요청 실패: %w", err)
		}
		defer resp.Body.Close()

		var apiResp ApiResponse
		if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
			return fmt.Errorf("응답 처리 실패: %w", err)
		}

		var downloadUrl, fileName string
		for _, item := range apiResp.Data {
			if strings.Contains(item.Name, "buridge_dy_product_daily_data") {
				downloadUrl = item.URL
				fileName = item.Name
				break
			}
		}
		if downloadUrl == "" || fileName == "" {
			return fmt.Errorf("유효한 파일 URL 또는 이름을 찾을 수 없음")
		}

		filePath := filepath.Join("/tmp", fileName)
		if err := downloadAvroFile(downloadUrl, filePath); err != nil {
			return fmt.Errorf("Avro 파일 다운로드 실패: %w", err)
		}

		s3Key := fmt.Sprintf("NewRank/buridge_dy_product_daily_data/%s", fileName)
		if err := uploadToS3(filePath, bucketName, s3Key); err != nil {
			return fmt.Errorf("S3 업로드 실패: %w", err)
		}

		return nil
	})

	if err != nil {
		fmt.Printf("Error in operation: %v\n", err)
		return events.APIGatewayProxyResponse{
			Body:       fmt.Sprintf("Failed to process: %s", err),
			StatusCode: 500,
		}, nil
	}

	return events.APIGatewayProxyResponse{
		Body:       "완료",
		StatusCode: 200,
	}, nil
}

func main() {

	lambda.Start(HandleRequest)
}
