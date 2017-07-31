/* Copyright (c) 2017 Parallels IP Holdings GmbH */

package snapshot_test

import (
	"testing"

	"github.com/kubernetes-incubator/apiserver-builder/pkg/test"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"k8s.io/client-go/rest"

	"git.sw.ru/ap/ap-api-snapshots/pkg/apis"
	"git.sw.ru/ap/ap-api-snapshots/pkg/client/clientset_generated/clientset"
	"git.sw.ru/ap/ap-api-snapshots/pkg/controller/sharedinformers"
	"git.sw.ru/ap/ap-api-snapshots/pkg/controller/snapshot"
	"git.sw.ru/ap/ap-api-snapshots/pkg/openapi"
)

var testenv *test.TestEnvironment
var config *rest.Config
var cs *clientset.Clientset
var shutdown chan struct{}
var controller *snapshot.SnapshotController
var si *sharedinformers.SharedInformers

func TestSnapshot(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecsWithDefaultAndCustomReporters(t, "Snapshot Suite", []Reporter{test.NewlineReporter{}})
}

var _ = BeforeSuite(func() {
	testenv = test.NewTestEnvironment()
	config = testenv.Start(apis.GetAllApiBuilders(), openapi.GetOpenAPIDefinitions)
	cs = clientset.NewForConfigOrDie(config)

	shutdown = make(chan struct{})
	si = sharedinformers.NewSharedInformers(config, shutdown)
	controller = snapshot.NewSnapshotController(config, si)
	controller.Run(shutdown)
})

var _ = AfterSuite(func() {
	close(shutdown)
	testenv.Stop()
})
