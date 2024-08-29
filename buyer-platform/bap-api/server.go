// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Server serves HTTP requests as a BAP in the ONDCnetwork.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	// "time"
	

	"cloud.google.com/go/pubsub"
	"github.com/benbjohnson/clock"
	log "github.com/golang/glog"

	"partner-innovation.googlesource.com/googleondcaccelerator.git/shared/clients/registryclient"
	"partner-innovation.googlesource.com/googleondcaccelerator.git/shared/clients/transactionclient"
	"partner-innovation.googlesource.com/googleondcaccelerator.git/shared/config"
	"partner-innovation.googlesource.com/googleondcaccelerator.git/shared/errorcode"
	"partner-innovation.googlesource.com/googleondcaccelerator.git/shared/middleware"
	"partner-innovation.googlesource.com/googleondcaccelerator.git/shared/models/model"
)

const psMsgIDHeader = "Pubsub-Message-ID"

var validate = model.Validator()

type server struct {
	mux               http.Handler
	port              int
}

func main() {
	flag.Set("alsologtostderr", "true")
	ctx := context.Background()

	configPath, ok := os.LookupEnv("CONFIG")
	if !ok {
		log.Exit("CONFIG env is not set")
	}

	log.Infof("CONFIG environment variable is set to: %s\n", configPath)

	log.Info("Attempting to read configuration file")
	conf, err := config.Read[config.BAPAPIConfig](configPath)
	if err != nil {
		log.Exitf("Failed to read configuration file: %v", err)
	}
	log.Info("Configuration file read successfully")

	registryClient, err := registryclient.New(conf.RegistryURL, conf.ONDCEnvironment)
	if err != nil {
		log.Exit(err)
	}

	srv, err := initServer(ctx, conf, nil, registryClient, nil, clock.New())
	if err != nil {
		log.Exit(err)
	}
	log.Info("Server initialization successs")

	err = srv.serve()
	if errors.Is(err, http.ErrServerClosed) {
		log.Info("Server is closed")
	} else if err != nil {
		log.Exitf("Serving failed: %v", err)
	}
}

func initServer(ctx context.Context, conf config.BAPAPIConfig, pubsubClient *pubsub.Client, registryClient middleware.RegistryClient, transactionClient *transactionclient.Client, clk clock.Clock) (*server, error) {
	if registryClient == nil {
		return nil, errors.New("init server: registry client is nil")
	}

	srv := &server{
		port:              conf.Port,
	}

	mux := http.NewServeMux()
	for _, e := range [10]struct {
		path    string
		handler http.HandlerFunc
	}{
		{"/on_search", srv.onSearchHandler},
		{"/on_select", srv.onSelectHandler},
		{"/on_init", srv.onInitHandler},
		{"/on_confirm", srv.onConfirmHandler},
		{"/on_status", srv.onStatusHandler},
		{"/on_track", srv.onTrackHandler},
		{"/on_cancel", srv.onCancelHandler},
		{"/on_update", srv.onUpdateHandler},
		{"/on_rating", srv.onRatingHandler},
		{"/on_support", srv.onSupportHandler},
	} {
		mux.HandleFunc(e.path, e.handler)
	}

	srv.mux = middleware.Adapt(
		mux,
		middleware.NPAuthentication(registryClient, clk, errorcode.RoleBuyerApp, conf.SubscriberID),
		middleware.OnlyPostMethod(),
		middleware.Logging(),
	)

	return srv, nil
}

// decodeAndValidate decodes JSON body and validate the payload.
func decodeAndValidate(body []byte, payload any) error {
	if err := json.Unmarshal(body, &payload); err != nil {
		return err
	}
	return validate.Struct(payload)
}

// nackResponse returns an appropriate status code and response body for invalid request body.
func nackResponse(w http.ResponseWriter, errType, errCode string) {
	res := model.AckResponse{
		Message: &model.MessageAck{
			Ack: &model.Ack{
				Status: "NACK",
			},
		},
		Error: &model.Error{
			Type: errType,
			Code: &errCode,
		},
	}

	resJSON, err := json.Marshal(res)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	w.Write(resJSON)
}

// ackResponse returns an appropriate status code and response body for valid request body.
func ackResponse(w http.ResponseWriter) {
	res := model.AckResponse{
		Message: &model.MessageAck{
			Ack: &model.Ack{
				Status: "ACK",
			},
		},
	}

	resJSON, err := json.Marshal(res)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(resJSON)
}

func (s *server) serve() error {
	addr := fmt.Sprintf(":%d", s.port)
	log.Info("Server is serving")
	return http.ListenAndServe(addr, s.mux)
}

// genericHandler can handles all kind of ONDC request.
func genericHandler[R model.BAPRequest](s *server, action string, w http.ResponseWriter, r *http.Request) {

	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Errorf("Read request body: %v", err)
		return
	}

	var payload R
	if err := decodeAndValidate(body, &payload); err != nil {
		log.Errorf("Request body is invalid: %v", err)
		errCodeInt, ok := errorcode.Lookup(errorcode.RoleSellerApp, errorcode.ErrInvalidRequest)
		if !ok {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		errType := "JSON-SCHEMA-ERROR"
		errCode := strconv.Itoa(errCodeInt)

		nackResponse(w, errType, errCode)
		return
	}

	ackResponse(w)
	log.Infof("Successfully ack request: TransactionID: %q, MessageID: %q", *payload.GetContext().TransactionID, *payload.GetContext().MessageID)
}

func (s *server) onSearchHandler(w http.ResponseWriter, r *http.Request) {
	genericHandler[model.OnSearchRequest](s, "on_search", w, r)
}

func (s *server) onSelectHandler(w http.ResponseWriter, r *http.Request) {
	genericHandler[model.OnSelectRequest](s, "on_select", w, r)
}

func (s *server) onInitHandler(w http.ResponseWriter, r *http.Request) {
	genericHandler[model.OnInitRequest](s, "on_init", w, r)
}

func (s *server) onConfirmHandler(w http.ResponseWriter, r *http.Request) {
	genericHandler[model.OnConfirmRequest](s, "on_confirm", w, r)
}

func (s *server) onStatusHandler(w http.ResponseWriter, r *http.Request) {
	genericHandler[model.OnStatusRequest](s, "on_status", w, r)
}

func (s *server) onTrackHandler(w http.ResponseWriter, r *http.Request) {
	genericHandler[model.OnTrackRequest](s, "on_track", w, r)
}

func (s *server) onCancelHandler(w http.ResponseWriter, r *http.Request) {
	genericHandler[model.OnCancelRequest](s, "on_cancel", w, r)
}

func (s *server) onUpdateHandler(w http.ResponseWriter, r *http.Request) {
	genericHandler[model.OnUpdateRequest](s, "on_update", w, r)
}

func (s *server) onRatingHandler(w http.ResponseWriter, r *http.Request) {
	genericHandler[model.OnRatingRequest](s, "on_rating", w, r)
}

func (s *server) onSupportHandler(w http.ResponseWriter, r *http.Request) {
	genericHandler[model.OnSupportRequest](s, "on_support", w, r)
}
