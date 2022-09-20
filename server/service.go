package server

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/odpf/predator/audit"
	"github.com/odpf/predator/auditor"
	"github.com/odpf/predator/bigqueryjob"
	"github.com/odpf/predator/metric/field"
	"github.com/odpf/predator/metric/table"
	"github.com/odpf/predator/profile"
	"github.com/odpf/predator/status"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/odpf/predator/metadata/uniqueconstraint"
	"github.com/odpf/predator/publisher"
	"github.com/odpf/predator/publisher/message"
	"github.com/odpf/predator/stats"
	"github.com/odpf/predator/stats/builder"
	"github.com/odpf/predator/stats/client"

	"github.com/odpf/predator/entity"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
	"github.com/gorilla/handlers"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"

	"github.com/odpf/predator/api/router"
	"github.com/odpf/predator/conf"
	"github.com/odpf/predator/metadata"
	"github.com/odpf/predator/metric"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/query"
	"github.com/odpf/predator/tolerance"
)

const MetadataCacheExpirationSeconds = 180

//HTTPService is predator as http service
type HTTPService struct {
	statsClient      stats.Client
	server           *http.Server
	auditService     protocol.AuditService
	profileService   protocol.ProfileService
	auditPublisher   protocol.Publisher
	profilePublisher protocol.Publisher
}

//Start to start http service
func (s *HTTPService) Start() <-chan bool {
	terminated := make(chan bool, 1)

	if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Could not listen on %v \nCause %v", s.server.Addr, err)
	}

	go func() {
		exitSignal := make(chan os.Signal, 1)
		signal.Notify(exitSignal, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
		<-exitSignal
		defer close(terminated)
	}()
	return terminated
}

//Shutdown listen to exit signal and shutdown service
func (s *HTTPService) Shutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	log.Println("server is shutting down")
	s.server.SetKeepAlivesEnabled(false)
	if err := s.server.Shutdown(ctx); err != nil {
		log.Fatalf("Unable to gracefully shutdown the server: %v\n", err)
	}

	err := s.profileService.WaitAll(ctx)
	if err != nil {
		log.Fatal(err)
	}
	err = s.profilePublisher.Close(ctx)
	if err != nil {
		log.Fatal(err)
	}
	err = s.auditPublisher.Close(ctx)
	if err != nil {
		log.Fatal(err)
	}

	s.statsClient.Close()
}

//StartService to start predator service
func StartService(confFile *conf.ConfigFile, version string) {
	config, err := conf.LoadConfig(confFile)
	if err != nil {
		log.Println(err)
		return
	}
	log.Printf("starting predator service %s", version)

	db, err := newDatabase(config.Database)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if err := db.Close(); err != nil {
			log.Println(err)
		}
	}()

	entityStore := entity.NewStore(db, "entity")

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	bqClient, err := newBigqueryClient(ctx, config.GCPProjectID, config.GCPServiceAcc)
	if err != nil {
		log.Println(err)
		return
	}

	// uniqueConstraintStore will be deprecated soon
	uniqueConstraintDictionaryStoreFactory := uniqueconstraint.NewDictionaryStoreFactory()
	uniqueConstraintStoreFactory := uniqueconstraint.NewStoreFactory(uniqueConstraintDictionaryStoreFactory)
	uniqueConstraintStore, err := uniqueConstraintStoreFactory.CreateUniqueConstraintStore(config.UniqueConstraintURL)
	if err != nil {
		log.Println(err)
		return
	}

	directMetadataStore := metadata.NewStore(bqClient, uniqueConstraintStore)
	metadataStore := metadata.NewCachedStore(MetadataCacheExpirationSeconds, directMetadataStore)

	gcsC, err := storage.NewClient(context.Background())
	if err != nil {
		log.Println(err)
		return
	}
	gcsClient := stiface.AdaptClient(gcsC)
	fileStoreFactory := tolerance.NewFileStoreFactory(gcsClient)
	pathResolverFactory := tolerance.NewPathResolverFactory(entityStore)
	toleranceStoreFactory := tolerance.NewFactory(pathResolverFactory, fileStoreFactory)
	toleranceStore, err := toleranceStoreFactory.Create(config.ToleranceURL, config.MultiTenancyEnabled)

	if err != nil {
		log.Println(err)
		return
	}

	metricSpecGenerator := metric.NewBasicMetricSpecGenerator(toleranceStore, metadataStore)
	qualityMetricSpecGenerator := metric.NewQualityMetricSpecGenerator(metadataStore, toleranceStore)

	statsdConf := &client.StatsdConfig{
		AppName: "predator",
		Port:    8125,
	}

	statsClient, err := client.NewNetStatsd(statsdConf, nil)
	if err != nil {
		log.Fatal(err)
		return
	}

	statsClientBuilder := stats.ClientBuilder(builder.NewMultiTenancy(config.MultiTenancyEnabled, entityStore, statsClient))
	statsClientBuilder = statsClientBuilder.
		WithEnvironment(config.Environment)

	key, err := readGitKey(config.GitAuthPrivateKeyPath)
	if err != nil {
		fmt.Println("error:", err)
		return
	}

	gitAuthPrivateKey, err := tolerance.GitAuthPrivateKey(key)
	if err != nil {
		log.Fatal(err)
		return
	}

	gitRepositoryFactory := tolerance.NewGitRepositoryFactory(gitAuthPrivateKey)
	uploadFactory := tolerance.NewUploadFactory(config.MultiTenancyEnabled, entityStore, toleranceStoreFactory, toleranceStore, gitRepositoryFactory, statsClientBuilder, metadataStore)

	statusStore := status.NewStore(db, "status")

	profileStore := profile.NewStore(db, "profile", statusStore)
	metricStore := profile.NewMetricStore(db, "metric")

	bqJob := bigqueryjob.NewStore(db, "bigquery_job")
	queryExecutor := query.NewBigqueryExecutor(bqClient, bqJob, profileStore, statsClientBuilder)

	fieldProfiler := field.New(queryExecutor, metadataStore)
	tableProfiler := table.New(queryExecutor, metadataStore)
	basicMetricProfiler := metric.NewBasicMetricProfiler(tableProfiler, fieldProfiler, profileStore, statsClientBuilder)
	basicMetricGenerator := metric.NewDefaultGenerator(metricSpecGenerator, basicMetricProfiler, metricStore)

	qualityMetricProfiler := metric.NewQualityMetricProfiler(metricStore, profileStore, statsClientBuilder)
	qualityMetricGenerator := metric.NewDefaultGenerator(qualityMetricSpecGenerator, qualityMetricProfiler, metricStore)

	profileStatisticGenerator := metric.NewDefaultProfileStatisticGenerator(metadataStore, queryExecutor, profileStore)
	metricGenerator := metric.NewMultistageGenerator([]protocol.MetricGenerator{basicMetricGenerator, qualityMetricGenerator}, profileStatisticGenerator)

	messageProviderFactory := message.NewProviderFactory(profileStore, metadataStore)

	sinkFactory := publisher.SinkFactory{}
	profileSinkConfig := &protocol.SinkConfig{
		Type:   protocol.Kafka,
		Broker: config.Publisher.Profile.Broker,
		Topic:  config.Publisher.Profile.Topic,
	}
	if config.Publisher.Profile.Topic == "" {
		profileSinkConfig.Type = protocol.Console
	}
	profileKafkaSink := sinkFactory.Create(profileSinkConfig)
	profilePublisher := publisher.NewPublisher(profileKafkaSink)

	profileService := profile.NewService(profileStore, metricGenerator, profilePublisher, messageProviderFactory, statusStore, statsClientBuilder)

	auditStore := audit.NewStore(db, "audit", statusStore)
	auditResultStore := audit.NewResultStore(db, "audit_result")
	ruleValidator := auditor.NewDefaultRuleValidator()
	metricAuditor := auditor.New(toleranceStore, ruleValidator, metadataStore, metricStore)

	auditSinkConfig := &protocol.SinkConfig{
		Type:   protocol.Kafka,
		Broker: config.Publisher.Audit.Broker,
		Topic:  config.Publisher.Audit.Topic,
	}
	if config.Publisher.Profile.Topic == "" {
		auditSinkConfig.Type = protocol.Console
	}
	auditKafkaSink := sinkFactory.Create(auditSinkConfig)
	auditPublisher := publisher.NewPublisher(auditKafkaSink)
	auditService := audit.NewService(profileStore, auditStore, auditResultStore, metricAuditor, auditPublisher, messageProviderFactory, metadataStore, statsClientBuilder)

	sqlExpressionFactory := query.NewSQLExpressionFactory(metadataStore)
	auditSummaryFactory := audit.NewAuditSummaryFactory(toleranceStore)

	v1beta1Routes := router.NewV1Beta1RouteGroup(profileService, auditService, toleranceStore, entityStore, uploadFactory, auditSummaryFactory, sqlExpressionFactory, metricStore)

	apiRouter := router.New(v1beta1Routes)

	PORT := config.Port
	fmt.Printf("Listening on PORT: %v \n", PORT)

	hostPort := fmt.Sprintf(":%v", PORT)
	server := createServer(hostPort, handlers.LoggingHandler(os.Stdout, apiRouter))

	service := &HTTPService{
		statsClient:      statsClient,
		server:           server,
		auditPublisher:   auditPublisher,
		profilePublisher: profilePublisher,
		auditService:     auditService,
		profileService:   profileService,
	}
	<-service.Start()
	service.Shutdown()
	log.Println("server stopped")
}

func readGitKey(keyFilePath string) ([]byte, error) {
	if keyFilePath != "" {
		keyFileContent, err := ioutil.ReadFile(keyFilePath)
		if err != nil {
			return nil, err
		}

		//to avoid double base64 encoded key
		if !strings.Contains(string(keyFileContent), "KEY") {
			return base64.StdEncoding.DecodeString(string(keyFileContent))
		}

		return keyFileContent, nil
	}
	return []byte(""), nil
}

func createServer(addr string, router http.Handler) *http.Server {
	return &http.Server{
		Addr:         addr,
		Handler:      router,
		WriteTimeout: 5 * time.Minute,
	}
}

func newDatabase(dbConf *conf.Database) (*gorm.DB, error) {
	dbHost := dbConf.Host
	dbPort := dbConf.Port
	dbName := dbConf.Name
	dbUser := dbConf.User
	dbPass := dbConf.Pass

	connString := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", dbUser, dbPass, dbHost, dbPort, dbName)
	db, err := gorm.Open("postgres", connString)

	if err != nil {
		return nil, fmt.Errorf("error connection to database %v", err)
	}

	return db, nil
}

func newBigqueryClient(ctx context.Context, projectID string, acc string) (bqiface.Client, error) {
	var options []option.ClientOption
	if acc != "" {
		cred, err := google.CredentialsFromJSON(ctx, []byte(acc), bigquery.Scope)
		if err != nil {
			return nil, fmt.Errorf("failed to read credentials: %w", err)
		}

		options = append(options, option.WithCredentials(cred))
	}

	client, err := bigquery.NewClient(ctx, projectID, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create BQ client: %w", err)
	}

	return bqiface.AdaptClient(client), nil
}
