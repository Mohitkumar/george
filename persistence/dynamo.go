package persistence

import (
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/mohitkumar/george/s2"
)

const MAX_CONCURRENT_QUERY = 20

type DynamoConfig struct {
	AWSRegion            string
	EndPoint             string
	TableName            string
	GeoHashKeyColumnName string
	GeoHashColumnName    string
	GeoHashKeyLength     uint16
	RCU                  int64
	WCU                  int64
}

type dynamoStore struct {
	DynamoConfig
	session *session.Session
}

var _ = (*DB)(nil)

func NewDynamoStore(config DynamoConfig) (*dynamoStore, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(config.AWSRegion),
		Endpoint:    aws.String(config.EndPoint),
		Credentials: credentials.NewStaticCredentials("fakeMyKeyId", "fff", ""),
	})
	if err != nil {
		return nil, err
	}
	store := &dynamoStore{
		DynamoConfig: config,
		session:      sess,
	}

	return store, nil
}

func (store *dynamoStore) CreateTable(config DynamoConfig) error {

	tableInput := &dynamodb.CreateTableInput{
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(config.GeoHashKeyColumnName),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String(config.GeoHashColumnName),
				KeyType:       aws.String("RANGE"),
			},
		},
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(config.GeoHashKeyColumnName),
				AttributeType: aws.String("N"),
			},
			{
				AttributeName: aws.String(config.GeoHashColumnName),
				AttributeType: aws.String("N"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(config.RCU),
			WriteCapacityUnits: aws.Int64(config.WCU),
		},
		TableName: aws.String(config.TableName),
	}
	svc := dynamodb.New(store.session)
	_, err := svc.CreateTable(tableInput)

	if err != nil {
		return err
	}
	return nil
}

func (store *dynamoStore) Put(latitude float64, longitude float64, data map[string]interface{}) error {
	data["latitude"] = latitude
	data["longitude"] = longitude
	geoHash := s2.ToGeoHash(latitude, longitude)

	geoHashKey := s2.ExtractHashKey(geoHash, int(store.GeoHashKeyLength))

	attrMap, err := dynamodbattribute.MarshalMap(data)
	if err != nil {
		return err
	}
	geoHahsStr := strconv.FormatUint(geoHash, 10)
	geoHashKeyStr := strconv.FormatUint(geoHashKey, 10)
	attrMap[store.GeoHashColumnName] = &dynamodb.AttributeValue{N: &geoHahsStr}
	attrMap[store.GeoHashKeyColumnName] = &dynamodb.AttributeValue{N: &geoHashKeyStr}

	input := dynamodb.PutItemInput{
		Item:      attrMap,
		TableName: aws.String(store.TableName),
	}

	svc := dynamodb.New(store.session)
	_, err = svc.PutItem(&input)

	if err != nil {
		return err
	}
	return nil
}

func (store *dynamoStore) Get(latitude float64, longitude float64) (map[string]interface{}, error) {
	geoHash := s2.ToGeoHash(latitude, longitude)
	geoHashKey := s2.ExtractHashKey(geoHash, int(store.GeoHashKeyLength))

	geoHahsStr := strconv.FormatUint(geoHash, 10)
	geoHashKeyStr := strconv.FormatUint(geoHashKey, 10)

	keyMap := make(map[string]*dynamodb.AttributeValue)
	keyMap[store.GeoHashKeyColumnName] = &dynamodb.AttributeValue{N: &geoHashKeyStr}
	keyMap[store.GeoHashColumnName] = &dynamodb.AttributeValue{N: &geoHahsStr}
	input := dynamodb.GetItemInput{
		Key:       keyMap,
		TableName: aws.String(store.TableName),
	}
	svc := dynamodb.New(store.session)
	out, err := svc.GetItem(&input)
	if err != nil {
		return nil, err
	}
	outputmap := make(map[string]interface{})

	err = dynamodbattribute.UnmarshalMap(out.Item, &outputmap)
	if err != nil {
		return nil, err
	}
	return outputmap, nil
}

func (store *dynamoStore) RadiusQuery(latitude float64, longitude float64, radius float64) ([]map[string]interface{}, error) {
	result := make([]map[string]interface{}, 0)
	svc := dynamodb.New(store.session)
	sem := make(chan int, MAX_CONCURRENT_QUERY)
	queries := store.getQueries(latitude, longitude, radius)
	var wg sync.WaitGroup
	for _, query := range queries {
		wg.Add(1)
		sem <- 1
		go func(query dynamodb.QueryInput) {
			defer wg.Done()
			queryResult := store.excuteQuery(svc, &query)
			result = append(result, queryResult...)
			<-sem
		}(query)
	}
	wg.Wait()
	return result, nil
}

func (store *dynamoStore) excuteQuery(service *dynamodb.DynamoDB, input *dynamodb.QueryInput) []map[string]interface{} {
	result := make([]map[string]interface{}, 0)
	for {
		output, err := service.Query(input)
		if err == nil {
			for _, item := range output.Items {
				outputmap := make(map[string]interface{})
				err = dynamodbattribute.UnmarshalMap(item, &outputmap)
				if err == nil {
					result = append(result, outputmap)
				}
			}
			input = input.SetExclusiveStartKey(output.LastEvaluatedKey)
			if output.LastEvaluatedKey == nil {
				break
			}
		} else {
			break
		}
	}
	return result
}

func (store *dynamoStore) getQueries(latitude float64, longitude float64, radius float64) []dynamodb.QueryInput {
	hashRanges := s2.CreateHashRanges(latitude, longitude, radius)
	queries := make([]dynamodb.QueryInput, 0)

	for _, hashRange := range hashRanges {
		splits := hashRange.Split(int(store.GeoHashKeyLength))
		for _, rn := range splits {
			hashKey := s2.ExtractHashKey(rn.RangeMin, int(store.GeoHashKeyLength))
			hashKeystr := strconv.FormatUint(hashKey, 10)
			keyConditions := make(map[string]*dynamodb.Condition)
			attrValueList := make([]*dynamodb.AttributeValue, 0)
			attrValueList = append(attrValueList, &dynamodb.AttributeValue{N: aws.String(hashKeystr)})
			geoHashCondition := dynamodb.Condition{ComparisonOperator: aws.String("EQ"),
				AttributeValueList: attrValueList}
			keyConditions[store.GeoHashKeyColumnName] = &geoHashCondition

			minRangeStr := strconv.FormatUint(rn.RangeMin, 10)
			maxRangeStr := strconv.FormatUint(rn.RangeMax, 10)

			minRange := dynamodb.AttributeValue{N: &minRangeStr}
			maxRange := dynamodb.AttributeValue{N: &maxRangeStr}

			rangeCondition := dynamodb.Condition{ComparisonOperator: aws.String("BETWEEN"),
				AttributeValueList: []*dynamodb.AttributeValue{&minRange, &maxRange}}

			keyConditions[store.GeoHashColumnName] = &rangeCondition
			input := dynamodb.QueryInput{
				TableName:     aws.String(store.TableName),
				KeyConditions: keyConditions,
			}
			queries = append(queries, input)
		}
	}
	return queries
}
