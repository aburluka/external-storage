/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/golang/glog"
	"github.com/kubernetes-incubator/external-storage/lib/controller"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/pkg/api/resource"
	"k8s.io/client-go/pkg/api/v1"
	"k8s.io/client-go/pkg/types"
	"k8s.io/client-go/pkg/util/uuid"
	"k8s.io/client-go/pkg/util/wait"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/dustin/go-humanize"
	"github.com/kolyshkin/goploop-cli"
)

const (
	resyncPeriod              = 15 * time.Second
	provisionerName           = "kubernetes.io/virtuozzo-storage"
	exponentialBackOffOnError = false
	failedRetryThreshold      = 5
	provisionerIDAnn          = "vzFSProvisionerIdentity"
	vzShareAnn                = "vzShare"
)

type provisionOutput struct {
	Path string `json:"path"`
}

type vzFSProvisioner struct {
	// Kubernetes Client. Use to retrieve Ceph admin secret
	client kubernetes.Interface
	// Identity of this vzFSProvisioner, generated. Used to identify "this"
	// provisioner's PVs.
	identity types.UID
}

func newVzFSProvisioner(client kubernetes.Interface) controller.Provisioner {
	return &vzFSProvisioner{
		client:   client,
		identity: uuid.NewUUID(),
	}
}

var _ controller.Provisioner = &vzFSProvisioner{}

func initVstorage(options map[string]string, secret map[string][]byte) error {
	clusterName := string(secret["clusterName"][:len(secret["clusterName"])])
	auth := exec.Command("/usr/bin/vstorage", "-c", clusterName, "auth-node", "-P")
	var b bytes.Buffer
	b.Write(secret["clusterPassword"])
	auth.Stdout = nil
	auth.Stdin = &b
	auth.Stderr = nil
	if err := auth.Run(); err != nil {
		return err
	}

	mount := exec.Command("/usr/bin/vstorage-mount", "-c",
		clusterName, options["volumePath"], "-l", "/dev/null")
	return mount.Run()
}

func createPloop(options map[string]string) error {
	var (
		volumePath, volumeId, size string
	)

	for k, v := range options {
		switch k {
		case "volumePath":
			volumePath = v
		case "volumeId":
			volumeId = v
		case "size":
			size = v
		case "vzsReplicas":
		case "vzsFailureDomain":
		case "vzsEncoding":
		case "vzsTier":
		case "kubernetes.io/readwrite":
		case "kubernetes.io/fsType":
		default:
		}
	}

	if volumePath == "" {
		return fmt.Errorf("volumePath isn't specified")
	}

	if volumeId == "" {
		return fmt.Errorf("volumeId isn't specified")
	}

	if size == "" {
		return fmt.Errorf("size isn't specified")
	}

	// get a human readable size from the map
	bytes, _ := humanize.ParseBytes(size)

	// ploop driver takes kilobytes, so convert it
	volume_size := bytes / 1024

	ploop_path := options["volumePath"] + "/" + options["volumeId"]

	// make the base directory where the volume will go
	err := os.MkdirAll(ploop_path, 0700)
	if err != nil {
		return err
	}

	for k, v := range options {
		var err error
		attr := ""
		switch k {
		case "vzsReplicas":
			attr = "replicas"
		case "vzsTier":
			attr = "tier"
		case "vzsEncoding":
			attr = "encoding"
		case "vzsFailureDomain":
			attr = "failure-domain"
		}
		if attr != "" {
			cmd := "vstorage"
			args := []string{"set-attr", "-R", ploop_path,
				fmt.Sprintf("%s=%s", attr, v)}
			err = exec.Command(cmd, args...).Run()
		}

		if err != nil {
			os.RemoveAll(ploop_path)
			return fmt.Errorf("Unable to set %s to %s: %v", attr, v, err)
		}
	}

	// Create the ploop volume
	cp := ploop.CreateParam{Size: volume_size, File: ploop_path + "/" + options["volumeId"]}
	if err := ploop.Create(&cp); err != nil {
		return err
	}

	return nil
}

// Provision creates a storage asset and returns a PV object representing it.
func (p *vzFSProvisioner) Provision(options controller.VolumeOptions) (*v1.PersistentVolume, error) {
	var (
		capacity resource.Quantity
		labels   map[string]string
	)

	capacity = options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
	z := capacity.Value()

	if options.PVC.Spec.Selector != nil && options.PVC.Spec.Selector.MatchExpressions != nil {
		return nil, fmt.Errorf("claim Selector.matchExpressions is not supported")
	}
	share := fmt.Sprintf("kubernetes-dynamic-pvc-%s", uuid.NewUUID())

	glog.Infof("Add %s %s", share, capacity.Value())

	if options.PVC.Spec.Selector != nil && options.PVC.Spec.Selector.MatchLabels != nil {
		labels = options.PVC.Spec.Selector.MatchLabels
	}

	storage_class_options := map[string]string{}
	for k, v := range options.Parameters {
		storage_class_options[k] = v
	}

	storage_class_options["volumeId"] = share
	storage_class_options["size"] = fmt.Sprintf("%d", z)

	if labels != nil {
		for k, v := range labels {
			switch k {
			case "vzsReplicas":
				v = strings.Replace(v, ".", ":", 1)
				v = strings.Replace(v, ".", "/", 1)
				storage_class_options[k] = v
			case "vzsTier":
				fallthrough
			case "vzsEncoding":
				v = strings.Replace(v, ".", "+", 1)
				v = strings.Replace(v, ".", "/", 1)
				storage_class_options[k] = v
			case "vzsFailureDomain":
				storage_class_options[k] = v
			default:
				glog.Infof("Skip %s = %s", k, v)
			}
		}
	}

	secret, err := p.client.Core().Secrets(storage_class_options["secretNamespace"]).Get(storage_class_options["secretName"])
	if err != nil {
		return nil, err
	}

	if err := initVstorage(storage_class_options, secret.Data); err != nil {
		return nil, err
	}
	defer syscall.Unmount(storage_class_options["volumePath"], syscall.MNT_DETACH)

	if err := createPloop(storage_class_options); err != nil {
		return nil, err
	}

	pv := &v1.PersistentVolume{
		ObjectMeta: v1.ObjectMeta{
			Name: options.PVName,
			Annotations: map[string]string{
				provisionerIDAnn: string(p.identity),
				vzShareAnn:       share,
			},
			Labels: labels,
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: options.PersistentVolumeReclaimPolicy,
			AccessModes:                   options.PVC.Spec.AccessModes,
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)],
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				FlexVolume: &v1.FlexVolumeSource{
					Driver:  "virtuozzo/ploop",
					Options: storage_class_options,
				},
			},
		},
	}

	glog.Infof("successfully created virtuozzo storage share: %s", share)

	return pv, nil
}

// Delete removes the storage asset that was created by Provision represented
// by the given PV.
func (p *vzFSProvisioner) Delete(volume *v1.PersistentVolume) error {
	ann, ok := volume.Annotations[provisionerIDAnn]
	if !ok {
		return errors.New("identity annotation not found on PV")
	}
	if ann != string(p.identity) {
		return &controller.IgnoredError{"identity annotation on PV does not match ours"}
	}
	share, ok := volume.Annotations[vzShareAnn]
	if !ok {
		return errors.New("vz share annotation not found on PV")
	}

	options := volume.Spec.PersistentVolumeSource.FlexVolume.Options

	secret := map[string][]byte{
		"clusterName":     []byte(options["clusterName"]),
		"clusterPassword": []byte(options["clusterPassword"]),
	}
	if err := initVstorage(options, secret); err != nil {
		return err
	}

	path := options["volumePath"] + "/" + options["volumeId"]
	glog.Infof("Delete: %s", path)
	err := os.RemoveAll(path)
	if err != nil {
		return err
	}

	glog.Infof("successfully delete virtuozzo storage share: %s", share)

	return nil
}

var (
	master     = flag.String("master", "", "Master URL")
	kubeconfig = flag.String("kubeconfig", "", "Absolute path to the kubeconfig")
)

func main() {
	flag.Parse()
	flag.Set("logtostderr", "true")

	var config *rest.Config
	var err error
	if *master != "" || *kubeconfig != "" {
		config, err = clientcmd.BuildConfigFromFlags(*master, *kubeconfig)
	} else {
		config, err = rest.InClusterConfig()
	}

	if err != nil {
		glog.Fatalf("Failed to create config: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		glog.Fatalf("Failed to create client: %v", err)
	}

	// The controller needs to know what the server version is because out-of-tree
	// provisioners aren't officially supported until 1.5
	serverVersion, err := clientset.Discovery().ServerVersion()
	if err != nil {
		glog.Fatalf("Error getting server version: %v", err)
	}

	// Create the provisioner: it implements the Provisioner interface expected by
	// the controller
	vzFSProvisioner := newVzFSProvisioner(clientset)

	// Start the provision controller which will dynamically provision cephFS
	// PVs
	pc := controller.NewProvisionController(clientset, resyncPeriod, provisionerName, vzFSProvisioner, serverVersion.GitVersion, exponentialBackOffOnError, failedRetryThreshold, 2*resyncPeriod, resyncPeriod, resyncPeriod/2, 2*resyncPeriod)

	pc.Run(wait.NeverStop)
}
