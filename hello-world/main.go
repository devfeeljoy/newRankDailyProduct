package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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

func convertAndUploadToMongoDB(filePath string, mongoClient *mongo.Client, databaseName string, collectionName string) error {
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

	// MongoDB 컬렉션 선택
	collection := mongoClient.Database(databaseName).Collection(collectionName)

	var documents []interface{}
	var currentBatchSize int
	// Avro 레코드를 읽고 MongoDB에 저장
	for fr.Scan() {
		// 현재 레코드 읽기
		raw, err := fr.Read()
		if err != nil {
			return err
		}

		// 필요한 데이터만 추출 (raw는 이미 map[string]interface{} 타입으로 역직렬화된 것으로 가정)
		record, ok := raw.(map[string]interface{})
		if !ok {
			return fmt.Errorf("record is not a map")
		}

		// 필요한 데이터 변환 수행
		for key, value := range record {
			if valueMap, ok := value.(map[string]interface{}); ok {
				if stringValue, ok := valueMap["string"].(string); ok {
					record[key] = stringValue
				}
			}
		}

		// BSON으로 문서 변환 후 크기 추정
		bsonData, err := bson.Marshal(record)
		if err != nil {
			return err
		}
		bsonSize := len(bsonData)

		// 일정 크기에 도달하면 배치 삽입 실행
		if currentBatchSize+bsonSize > 15*1024*1024 {
			_, err = collection.InsertMany(context.Background(), documents)
			if err != nil {
				return err
			}
			documents = []interface{}{} // 슬라이스 초기화
			currentBatchSize = 0
		}
		// MongoDB 문서로 변환
		documents = append(documents, record)
		currentBatchSize += bsonSize
	}

	// 남은 문서 처리
	if len(documents) > 0 {
		_, err = collection.InsertMany(context.Background(), documents)
		if err != nil {
			return err
		}
	}
	return nil
}

// Lambda 핸들러 함수
func HandleRequest(ctx context.Context) (events.APIGatewayProxyResponse, error) {
	//mongoDB 연결
	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI("mongodb+srv://sparta:p8hDJNs18HANQ7eU@cluster0.5pjmefm.mongodb.net/?retryWrites=true&w=majority").SetServerAPIOptions(serverAPI)
	// Create a new client and connect to the server
	clientMongo, err := mongo.Connect(context.TODO(), opts)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err = clientMongo.Disconnect(context.TODO()); err != nil {
			panic(err)
		}
	}()
	// Send a ping to confirm a successful connection
	if err := clientMongo.Database("admin").RunCommand(context.TODO(), bson.D{{"ping", 1}}).Err(); err != nil {
		panic(err)
	}
	fmt.Println("Pinged your deployment. You successfully connected to MongoDB!")

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
		if strings.Contains(item.Name, "buridge_dy_opus_daily_data") {
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
	err = convertAndUploadToMongoDB(filePath, clientMongo, "NewRank", "opus_daily")

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
