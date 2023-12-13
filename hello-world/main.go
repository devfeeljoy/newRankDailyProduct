package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/lambda"

	"github.com/linkedin/goavro/v2"
)

const avroSchema = `{
 "type" : "record",
 "name" : "BuridgeAccountInfoAvro",
 "namespace" : "cn.newrank.api.dy.custom.project.system.biz.buridge.model.avro",
 "fields" : [ {
   "name" : "uid",
   "type" : [ "string", "null" ]
 }, {
   "name" : "account",
   "type" : [ "string", "null" ]
 }, {
   "name" : "nickname",
   "type" : [ "string", "null" ]
 }, {
   "name" : "introduction",
   "type" : [ "string", "null" ]
 }, {
   "name" : "avatar",
   "type" : [ "string", "null" ]
 }, {
   "name" : "gender",
   "type" : [ "string", "null" ]
 }, {
   "name" : "mcnName",
   "type" : [ "string", "null" ]
 }, {
   "name" : "enterpriseVerify",
   "type" : [ "string", "null" ]
 }, {
   "name" : "customVerify",
   "type" : [ "string", "null" ]
 }, {
   "name" : "dyFansNum",
   "type" : [ "string", "null" ]
 }, {
   "name" : "fansNum",
   "type" : [ "string", "null" ]
 }, {
   "name" : "likeNum",
   "type" : [ "string", "null" ]
 }, {
   "name" : "opusNum",
   "type" : [ "string", "null" ]
 }, {
   "name" : "homeUrl",
   "type" : [ "string", "null" ]
 }, {
   "name" : "newrankWeekIndex",
   "type" : [ "string", "null" ]
 }, {
   "name" : "age",
   "type" : [ "string", "null" ]
 }, {
   "name" : "mainCategory",
   "type" : [ "string", "null" ]
 }, {
   "name" : "ipLocation",
   "type" : [ "string", "null" ]
 }, {
   "name" : "location",
   "type" : [ "string", "null" ]
 }, {
   "name" : "firstCategory",
   "type" : [ "string", "null" ]
 }, {
   "name" : "secondCategory",
   "type" : [ "string", "null" ]
 }, {
   "name" : "updateTime",
   "type" : [ "string", "null" ]
 } ]
}`

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

func convertAndUploadToDynamoDB(filePath string) error {
	//Avro 코덱 생성
	codec, err := goavro.NewCodec(avroSchema)
	if err != nil {
		return err
	}

	// 파일 열기
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Avro 파일 열기
	fr, err := goavro.NewOCFReader(file)
	if err != nil {
		return err
	}

	//DynamoDB 세션 및 클라이언트 초기화
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	dynamoDB := dynamodb.New(sess)

	var writeRequests []*dynamodb.WriteRequest
	// Avro 레코드를 읽고 DynamoDB에 저장
	for fr.Scan() {
		// 현재 레코드 읽기
		raw, err := fr.Read()
		if err != nil {
			return err
		}

		// Raw 데이터를 바이너리로 변환
		binary, err := codec.BinaryFromNative(nil, raw)
		if err != nil {
			return err
		}

		// Binary 데이터를 네이티브 Go 타입으로 역직렬화
		native, _, err := codec.NativeFromBinary(binary)
		if err != nil {
			return err
		}

		// 필요한 데이터 변환 수행
		record, ok := native.(map[string]interface{})
		if !ok {
			return fmt.Errorf("record is not a map")
		}

		// 예를 들어, 'string' 타입의 필드를 처리
		for key, value := range record {
			if valueMap, ok := value.(map[string]interface{}); ok {
				if stringValue, ok := valueMap["string"].(string); ok {
					record[key] = stringValue
				}
			}
		}

		//DynamoDB 항목 매핑
		item, err := dynamodbattribute.MarshalMap(record)
		if err != nil {
			return err
		}

		writeRequest := &dynamodb.WriteRequest{PutRequest: &dynamodb.PutRequest{Item: item}}
		writeRequests = append(writeRequests, writeRequest)

		if len(writeRequests) >= 25 {
			err = batchWriteToDynamoDB(dynamoDB, "Influencer", writeRequests)
			if err != nil {
				return err
			}
			writeRequests = writeRequests[:0] // 슬라이스 초기화
		}
	}
	// 마지막 배치 처리
	if len(writeRequests) > 0 {
		err = batchWriteToDynamoDB(dynamoDB, "Influencer", writeRequests)
		if err != nil {
			return err
		}
	}
	return nil
}

func batchWriteToDynamoDB(dynamoDB *dynamodb.DynamoDB, tableName string, writeRequests []*dynamodb.WriteRequest) error {
	batchInput := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			tableName: writeRequests,
		},
	}

	_, err := dynamoDB.BatchWriteItem(batchInput)
	return err
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
	yesterday := time.Now().AddDate(0, 0, -2).Format("2006-01-02")

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

	if err != nil {
		return events.APIGatewayProxyResponse{
			Body:       err.Error(),
			StatusCode: 500,
		}, nil
	}

	var downloadUrl, fileName string
	for _, item := range apiResp.Data {
		if strings.Contains(item.Name, "buridge_dy_account_daily_data") {
			downloadUrl = item.URL
			fileName = item.Name
			break
		}
	}

	if downloadUrl == "" {
		return events.APIGatewayProxyResponse{
			Body:       "No matching data found",
			StatusCode: 404,
		}, nil
	}

	// 파일 경로 생성 (/tmp 폴더와 파일 이름 결합)
	filePath := filepath.Join("/tmp", fileName)
	// Avro 파일 다운로드
	err = downloadAvroFile(downloadUrl, filePath)
	if err != nil {
		return events.APIGatewayProxyResponse{
			Body:       fmt.Sprintf("Failed to download Avro file: %s", err),
			StatusCode: 500,
		}, nil
	}
	err = convertAndUploadToDynamoDB(filePath)

	if err != nil {
		return events.APIGatewayProxyResponse{
			Body:       fmt.Sprintf("DB save Fail: %s", err),
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
