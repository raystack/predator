package conf

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

//Database is database config
type Database struct {
	Host string
	Port int
	Name string
	User string
	Pass string
}

//Kafka is configuration of apache kafka client
type Kafka struct {
	Topic  string
	Broker []string
}

//Publisher is predator config to publish data to kafka
type Publisher struct {
	Profile *Kafka
	Audit   *Kafka
}

//Config is service config
type Config struct {
	Port          int
	GCPProjectID  string
	GCPServiceAcc string

	Database *Database

	Publisher *Publisher

	ToleranceURL        string
	UniqueConstraintURL string

	GitAuthPrivateKeyPath string

	//MultiTenancyEnabled this will affect how tolerance spec files stored and read
	//if MULTI_TENANCY_ENABLED env variable is NOT present the value will be false
	MultiTenancyEnabled bool

	//PodName name of replication pod
	PodName string
	//Deployment name of deployment
	Deployment string
	//Environment environment name where the instance deployed
	Environment string
}

//ConfigFile as the configuration
type ConfigFile struct {
	FilePath string
}

//LoadConfig load configuration
func LoadConfig(confFile *ConfigFile) (*Config, error) {
	if confFile.FilePath != "" {
		return loadEnvFile(confFile.FilePath)
	}
	return loadFromEnv()
}

func loadEnvFile(filePath string) (*Config, error) {
	if err := godotenv.Load(filePath); err != nil {
		return nil, err
	}
	return loadFromEnv()
}

func printEnv() {
	for _, e := range os.Environ() {
		fmt.Println(e)
	}
}

func loadFromEnv() (*Config, error) {
	printEnv()
	port, err := strconv.Atoi(os.Getenv("PORT"))
	if err != nil {
		return nil, err
	}

	kafkaBroker := strings.Split(os.Getenv("KAFKA_BROKER"), ",")

	var multiTenancyEnabled bool
	if envValue, set := os.LookupEnv("MULTI_TENANCY_ENABLED"); set {
		value, err := strconv.ParseBool(envValue)
		if err != nil {
			return nil, err
		}
		multiTenancyEnabled = value
	}

	dbHost := os.Getenv("DB_HOST")
	dbPort, err := strconv.Atoi(os.Getenv("DB_PORT"))
	if err != nil {
		return nil, err
	}
	dbName := os.Getenv("DB_NAME")
	dbUser := os.Getenv("DB_USER")
	dbPass := os.Getenv("DB_PASS")

	environmentValue := os.Getenv("ENVIRONMENT")

	podName := os.Getenv("POD_NAME")
	if podName == "" {
		podName = os.Getenv("POD_IP")
	}

	return &Config{
		Port:          port,
		GCPProjectID:  os.Getenv("BIGQUERY_PROJECT_ID"),
		GCPServiceAcc: os.Getenv("BQ_SERVICE_ACCOUNT"),
		Database: &Database{
			Host: dbHost,
			Port: dbPort,
			Name: dbName,
			User: dbUser,
			Pass: dbPass,
		},

		Publisher: &Publisher{
			Profile: &Kafka{
				Topic:  os.Getenv("PROFILE_KAFKA_TOPIC"),
				Broker: kafkaBroker,
			},
			Audit: &Kafka{
				Topic:  os.Getenv("AUDIT_KAFKA_TOPIC"),
				Broker: kafkaBroker,
			},
		},
		ToleranceURL:          os.Getenv("TOLERANCE_STORE_URL"),
		UniqueConstraintURL:   os.Getenv("UNIQUE_CONSTRAINT_STORE_URL"),
		MultiTenancyEnabled:   multiTenancyEnabled,
		GitAuthPrivateKeyPath: os.Getenv("GIT_AUTH_PRIVATE_KEY_PATH"),
		PodName:               podName,
		Deployment:            os.Getenv("DEPLOYMENT"),
		Environment:           environmentValue,
	}, err
}
