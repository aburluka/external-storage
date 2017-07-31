/* Copyright (c) 2017 Parallels IP Holdings GmbH */

package v1_test

import (
	"testing"

	"github.com/kubernetes-incubator/apiserver-builder/pkg/test"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"k8s.io/client-go/rest"

	"git.sw.ru/ap/ap-api-snapshots/pkg/apis"
	"git.sw.ru/ap/ap-api-snapshots/pkg/client/clientset_generated/clientset"
	"git.sw.ru/ap/ap-api-snapshots/pkg/openapi"
)

var testenv *test.TestEnvironment
var config *rest.Config
var cs *clientset.Clientset

func TestV1(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecsWithDefaultAndCustomReporters(t, "v1 Suite", []Reporter{test.NewlineReporter{}})
}

var _ = BeforeSuite(func() {
	testenv = test.NewTestEnvironment()
	config = testenv.Start(apis.GetAllApiBuilders(), openapi.GetOpenAPIDefinitions)
	cs = clientset.NewForConfigOrDie(config)
})

var _ = AfterSuite(func() {
	testenv.Stop()
})
