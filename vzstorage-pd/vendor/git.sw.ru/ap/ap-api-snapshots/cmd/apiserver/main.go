/* Copyright (c) 2017 Parallels IP Holdings GmbH */

package main

import (
	// Make sure glide gets these dependencies
	_ "github.com/go-openapi/loads"
	_ "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubernetes-incubator/apiserver-builder/pkg/cmd/server"
	_ "k8s.io/client-go/plugin/pkg/client/auth" // Enable cloud provider auth

	"git.sw.ru/ap/ap-api-snapshots/pkg/apis"
	"git.sw.ru/ap/ap-api-snapshots/pkg/openapi"
)

func main() {
	version := "v0"
	server.StartApiServer("/registry/ap.virtuozzo.com", apis.GetAllApiBuilders(), openapi.GetOpenAPIDefinitions, "Api", version)
}
