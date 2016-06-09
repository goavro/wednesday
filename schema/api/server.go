package api

import (
	"fmt"
	"net/http"

	avro "github.com/elodina/go-avro"
	"github.com/julienschmidt/httprouter"
	"github.com/yanzay/log"
	"github.com/yanzay/wednesday/auth"
	"github.com/yanzay/wednesday/schema/storage"
)

type Watcher interface {
	Watch(string)
}

type SchemaMessage struct {
	Schema string `json:"schema"`
}

type ApiServer struct {
	storage storage.Storage
	address string
	watcher Watcher

	multiuser bool
	topic     string
}

func NewApiServer(addr string, stor storage.Storage, watcher Watcher, multiuser bool, topic string) *ApiServer {
	server := &ApiServer{
		storage:   stor,
		address:   addr,
		watcher:   watcher,
		multiuser: multiuser,
		topic:     topic,
	}
	return server
}

func (as *ApiServer) Start() error {
	router := httprouter.New()
	router.GET("/schemas/ids/:id", as.auth(as.GetSchema))
	router.GET("/subjects", as.auth(as.GetSubjects))
	router.GET("/subjects/:subject/versions", as.auth(as.GetVersionList))
	router.GET("/subjects/:subject/versions/:version", as.auth(as.GetVersion))
	router.POST("/subjects/:subject/versions", as.auth(as.NewSchema))
	router.POST("/subjects/:subject", as.auth(as.CheckRegistered))
	router.POST("/compatibility/subjects/:subject/versions/:version", as.auth(as.CheckCompatibility))
	router.PUT("/config", as.auth(as.UpdateGlobalConfig))
	router.GET("/config", as.auth(as.GetGlobalConfig))
	router.PUT("/config/:subject", as.auth(as.UpdateSubjectConfig))
	router.GET("/config/:subject", as.auth(as.GetSubjectConfig))

	if as.multiuser {
		//router.POST("/users", as.admin(as.auth(as.CreateUser)))
	} else {
		as.watcher.Watch(as.topic)
	}
	fmt.Printf("Starting schema server at %s\n", as.address)
	return http.ListenAndServe(as.address, router)
}

func (as *ApiServer) auth(handler httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		var client string
		fmt.Println("URI:", r.RequestURI)
		if as.multiuser {
			name := r.Header.Get("X-Api-User")
			token := r.Header.Get("X-Api-Key")
			fmt.Println("Token:", token)
			fmt.Println("Name:", name)
			user, ok := as.storage.UserByName(name)
			if !ok || user.Token != token {
				authorized, err := auth.Authorize(name, token)
				if err != nil {
					registryError(w, ErrAuthStore, http.StatusInternalServerError, err)
					return
				}
				if !authorized {
					registryError(w, ErrUnauthorized, http.StatusForbidden, err)
					return
				}

				err = as.storage.AddUser(name, token, true)
				as.watcher.Watch(name)
				if err != nil {
					registryError(w, ErrAuthStore, http.StatusInternalServerError, err)
					return
				}

				as.storage.CreateUser(name, token, true)
			}
			client = name
		} else {
			client = as.topic
		}
		ps = append(ps, httprouter.Param{Key: "client", Value: client})
		handler(w, r, ps)
	}
}

func (as *ApiServer) admin(handler httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		name := r.Header.Get("X-Api-User")
		admin, err := auth.IsAdmin(name)
		if err != nil {
			registryError(w, ErrAuthStore, http.StatusInternalServerError, err)
		}
		if !admin {
			registryError(w, ErrUnauthorized, http.StatusForbidden, nil)
			return
		}
		handler(w, r, ps)
	}
}

func schemaValid(schema string) bool {
	log.Infof("Validating schema %s", schema)
	_, err := avro.ParseSchema(schema)
	if err != nil {
		log.Infof("Schema is invalid: %s", err)
		return false
	}

	return true
}

func schemaCompatible(toValidate string, existing string, compatibilityLevel string) bool {
	schemaToValidate := avro.MustParseSchema(toValidate)
	existingSchema := avro.MustParseSchema(existing)

	checker, ok := compatibilityCheckers[compatibilityLevel]
	if !ok {
		log.Warningf("Compatibility level %s does not exist", compatibilityLevel)
		return false
	}

	err := checker.Validate(schemaToValidate, existingSchema)
	if err != nil {
		log.Infof("Compatibility check for level %s did not pass: %s", compatibilityLevel, err)
		return false
	}

	return true
}
