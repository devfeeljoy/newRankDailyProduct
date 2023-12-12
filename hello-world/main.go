package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"net/http"
	"net/url"
	"os"
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

// Lambda 핸들러 함수
func HandleRequest(ctx context.Context) (events.APIGatewayProxyResponse, error) {
	apiKey := os.Getenv("key") // 환경 변수에서 API 키 가져오기
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
	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest("GET", apiUrl.String(), nil)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}

	// 헤더 설정
	req.Header.Set("key", apiKey)

	// API 요청
	resp, err := client.Do(req)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}
	defer resp.Body.Close()

	// 응답 처리
	var apiResp ApiResponse
	err = json.NewDecoder(resp.Body).Decode(&apiResp)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}

	// 응답 데이터를 JSON 문자열로 변환
	responseBody, err := json.Marshal(apiResp)
	if err != nil {
		return events.APIGatewayProxyResponse{
			Body:       err.Error(),
			StatusCode: 500,
		}, nil
	}

	return events.APIGatewayProxyResponse{
		Body:       string(responseBody),
		StatusCode: 200,
	}, nil
}

func main() {
	lambda.Start(HandleRequest)
}
