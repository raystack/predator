package router

import (
	"github.com/gorilla/mux"
	"github.com/odpf/predator/api/handler"
)

func New(v1beta1Routes RouteGroup) *mux.Router {

	router := mux.NewRouter().StrictSlash(true)

	router.
		Methods("GET").Path("/ping").
		Name("ping").
		Handler(handler.Ping())

	v1beta1Routes.RegisterHandler(router)

	return router
}

type RouteGroup interface {
	RegisterHandler(router *mux.Router)
}
