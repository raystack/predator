package router

import (
	"github.com/gorilla/mux"
	v1beta1 "github.com/odpf/predator/api/handler/v1beta1"
	"github.com/odpf/predator/protocol"
	"log"
	"net/http"
	"time"
)

//V1Beta1RouteGroup as a struct for v1beta1 route group
type V1Beta1RouteGroup struct {
	profileService       protocol.ProfileService
	auditService         protocol.AuditService
	toleranceStore       protocol.ToleranceStore
	entityStore          protocol.EntityStore
	uploadFactory        protocol.UploadFactory
	auditSummaryFactory  protocol.AuditSummaryFactory
	sqlExpressionFactory protocol.SQLExpressionFactory
	metricStore          protocol.MetricStore
}

//NewV1Beta1RouteGroup to construct v1beta1 route group
func NewV1Beta1RouteGroup(profileService protocol.ProfileService,
	auditService protocol.AuditService,
	toleranceStore protocol.ToleranceStore,
	entityStore protocol.EntityStore,
	uploadFactory protocol.UploadFactory,
	auditSummaryFactory protocol.AuditSummaryFactory,
	sqlExpressionFactory protocol.SQLExpressionFactory,
	metricStore protocol.MetricStore) *V1Beta1RouteGroup {
	return &V1Beta1RouteGroup{
		profileService:       profileService,
		auditService:         auditService,
		toleranceStore:       toleranceStore,
		entityStore:          entityStore,
		uploadFactory:        uploadFactory,
		auditSummaryFactory:  auditSummaryFactory,
		sqlExpressionFactory: sqlExpressionFactory,
		metricStore:          metricStore,
	}
}

//RegisterHandler to register handler for v1beta1
func (v *V1Beta1RouteGroup) RegisterHandler(router *mux.Router) {
	router.
		Methods("POST").Path("/v1beta1/profile/{profileID}/audit").
		Name("v1beta1_create_audit_task").
		Handler(v1beta1.Audit(v.auditService, v.profileService, v.auditSummaryFactory))

	router.Methods("POST").Path("/v1beta1/profile").
		Name("v1beta1_profile").
		Handler(v1beta1.Profile(v.profileService, v.sqlExpressionFactory))

	router.Methods("GET").Path("/v1beta1/profile/{profileID}").
		Name("v1beta1_get_profile").
		Handler(v1beta1.GetProfile(v.profileService, v.metricStore))

	router.Methods("GET").Path("/v1beta1/profile/{profileID}/log").
		Name("v1beta1_get_profile_log").
		Handler(v1beta1.GetProfileLog(v.profileService))

	router.
		Methods("POST").Path("/v1beta1/entity/{entityID}").
		Name("v1beta1_upsert_entity").
		Handler(v1beta1.CreateUpdateEntity(v.entityStore))

	router.
		Methods("GET").Path("/v1beta1/entity").
		Name("v1beta1_get_all_entities").
		Handler(v1beta1.GetAllEntities(v.entityStore))

	router.
		Methods("POST").Path("/v1beta1/spec/upload").
		Name("v1beta1_upload_spec").
		Handler(v1beta1.Upload(v.uploadFactory))

	router.
		Methods("GET").Path("/wait").
		Name("wait").
		HandlerFunc(
			func(writer http.ResponseWriter, request *http.Request) {
				time.Sleep(2 * time.Minute)
				_, err := writer.Write([]byte("pong"))
				log.Println(err)
			})
}
