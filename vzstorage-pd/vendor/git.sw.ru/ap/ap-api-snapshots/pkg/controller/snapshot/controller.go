/* Copyright (c) 2017 Parallels IP Holdings GmbH */
// vim:tabstop=4

package snapshot

import (
	"crypto/md5"
	"flag"
	"fmt"
	"path/filepath"

	"github.com/golang/glog"

	"github.com/kubernetes-incubator/apiserver-builder/pkg/controller"
	apiv1 "k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"git.sw.ru/ap/ap-api-snapshots/pkg/apis/snapshots/v1"
	"git.sw.ru/ap/ap-api-snapshots/pkg/client/clientset_generated/clientset"
	snapshotsV1 "git.sw.ru/ap/ap-api-snapshots/pkg/client/clientset_generated/clientset/typed/snapshots/v1"
	listers "git.sw.ru/ap/ap-api-snapshots/pkg/client/listers_generated/snapshots/v1"
	"git.sw.ru/ap/ap-api-snapshots/pkg/controller/sharedinformers"
)

// +controller:group=snapshots,version=v1,kind=Snapshot,resource=snapshots
type SnapshotControllerImpl struct {
	// informer listens for events about Snapshot
	informer cache.SharedIndexInformer

	// lister indexes properties about Snapshot
	lister listers.SnapshotLister

	client *snapshotsV1.SnapshotsV1Client

	kubeClient *kubernetes.Clientset

	stopWatchingPods chan struct{}

	pods map[string]*v1.Snapshot
}

func (c *SnapshotControllerImpl) updatePod(obj interface{}, obj2 interface{}) {
	pod := obj2.(*apiv1.Pod)
	glog.Infof("Update: %s %v\n", pod.Name, pod.Status.Phase)

	if pod.Status.Phase == apiv1.PodFailed || pod.Status.Phase == apiv1.PodSucceeded {
		snap, ok := c.pods[pod.Name]
		if ok {
			snap, err := c.client.Snapshots(snap.ObjectMeta.Namespace).Get(snap.Name, meta_v1.GetOptions{})
			if err != nil {
				glog.Errorf("Unable to get the %s snapshot: %v", snap.Name, err)
				return
			}

			podsClient := c.kubeClient.CoreV1Client.Pods(pod.ObjectMeta.Namespace)

			var limitBytes int64 = 1 << 20
			req := podsClient.GetLogs(pod.Name, &apiv1.PodLogOptions{LimitBytes: &limitBytes})
			b, err := req.DoRaw()
			if err != nil {
				glog.Errorf("Unable to get logs for %s(%s): %v\n", snap.Name, pod.Name, err)
			} else {
				glog.Infof("logs (%s):\n%s\nEND\n", snap.Name, b)
			}

			if pod.Status.Phase == apiv1.PodSucceeded {
				if snap.Status.Phase == v1.SnapshotTerminating {
					glog.Infof("The %s snapshot was deleted\n", snap.Name)
					period := int64(0)
					opt := meta_v1.DeleteOptions{GracePeriodSeconds: &period}
					err = c.client.Snapshots(snap.ObjectMeta.Namespace).Delete(snap.Name, &opt)
					if err != nil {
						glog.Errorf("Unable to delete the %s snapshot: %v\n", snap.Name, err)
						snap.Status.Phase = v1.SnapshotFailed
					}
				} else {
					snap.Status.Phase = v1.SnapshotAvailable
					snap.Status.PodName = ""
				}
			} else {
				snap.Status.Phase = v1.SnapshotFailed
				snap.Status.Message = "Unable to create snapshot"
			}

			delete(c.pods, pod.Name)

			if snap.Status.Phase != v1.SnapshotTerminating {
				_, err = c.client.Snapshots(snap.ObjectMeta.Namespace).UpdateStatus(snap)
				if err != nil {
					glog.Errorf("Unable to update the %s snapshot: %v", snap.Name, err)
					return
				}
			}

			glog.Infof("Delete pod: %s\n", pod.Name)
			err = podsClient.Delete(pod.Name, &meta_v1.DeleteOptions{})
			if err != nil {
				glog.Errorf("Unable to delete the %s pod: %v\n", pod.Name, err)
			}
		}
	}
}

func (c *SnapshotControllerImpl) watchPods() {
	kubeLabelSelector, err := labels.Parse("service=vzsnap")
	if err != nil {
		glog.Errorf("Unable to parse labels: %v\n", err)
		return
	}

	source := NewListWatchFromClient(c.kubeClient.Core().RESTClient(), "pods", apiv1.NamespaceDefault, fields.Everything(), kubeLabelSelector)

	_, controller := cache.NewInformer(source, &apiv1.Pod{}, 0, cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod := obj.(*apiv1.Pod)
			glog.Infof("Add pod: %s\n", pod.Name)
		},
		UpdateFunc: c.updatePod,

		DeleteFunc: func(obj interface{}) {
			pod := obj.(*apiv1.Pod)
			glog.Infof("Delete pod: %s\n", pod.Name)
		},
	})
	c.stopWatchingPods = make(chan struct{})

	go controller.Run(c.stopWatchingPods)

}

// Init initializes the controller and is called by the generated code
// Registers eventhandlers to enqueue events
// config - client configuration for talking to the apiserver
// si - informer factory shared across all controllers for listening to events and indexing resource properties
// queue - message queue for handling new events.  unique to this controller.
func (c *SnapshotControllerImpl) Init(
	config *rest.Config,
	si *sharedinformers.SharedInformers,
	queue workqueue.RateLimitingInterface) {

	flag.Set("logtostderr", "true")

	// Set the informer and lister for subscribing to events and indexing snapshots labels
	i := si.Factory.Snapshots().V1().Snapshots()
	c.informer = i.Informer()
	c.lister = i.Lister()
	c.pods = make(map[string]*v1.Snapshot)

	cs := clientset.NewForConfigOrDie(config)
	c.client = cs.SnapshotsV1Client

	// Add an event handler to enqueue a message for snapshots adds / updates
	c.informer.AddEventHandler(&controller.QueueingEventHandler{queue})

	c.kubeClient = kubernetes.NewForConfigOrDie(config)

	c.watchPods()

	list, err := c.client.Snapshots("").List(meta_v1.ListOptions{})
	if err != nil {
		glog.Errorf("Unable to list snapshots: %v\n", err)
		return
	}

	glog.Infoln("Collect snapshots...")
	for _, s := range list.Items {
		glog.Infof("snap: %s\n", s.Name)
		if s.Status.Phase == v1.SnapshotPending {
			c.Reconcile(&s)
		} else if s.Status.Phase == v1.SnapshotScheduled {
			c.pods[s.Status.PodName] = &s

			podsClient := c.kubeClient.CoreV1Client.Pods(apiv1.NamespaceDefault)
			pod, err := podsClient.Get(s.Status.PodName, meta_v1.GetOptions{})
			if err != nil {
				glog.Errorf("Unable to find the %d pod: %v", s.Status.PodName, err)
				s.Status.Phase = v1.SnapshotFailed
				s.Status.Message = "Unable to find pod"
				_, err := c.client.Snapshots(s.ObjectMeta.Namespace).UpdateStatus(&s)
				if err != nil {
					glog.Errorf("Unable to update status for %s: %v\n", s.Name, err)
				}
				continue
			}
			c.updatePod(nil, pod)
		}
	}
}

// NewListWatchFromClient creates a new ListWatch from the specified client, resource, namespace, field selector and label selector.
// Extends cache.NewListWatchFromClient to support labelSelector
func NewListWatchFromClient(c cache.Getter, resource string, namespace string, fieldSelector fields.Selector, labelSelector labels.Selector) *cache.ListWatch {
	listFunc := func(options meta_v1.ListOptions) (runtime.Object, error) {
		options.FieldSelector = fieldSelector.String()
		options.LabelSelector = labelSelector.String()
		return c.Get().
			Namespace(namespace).
			Resource(resource).
			VersionedParams(&options, meta_v1.ParameterCodec).
			Do().
			Get()
	}
	watchFunc := func(options meta_v1.ListOptions) (watch.Interface, error) {
		options.Watch = true
		options.FieldSelector = fieldSelector.String()
		options.LabelSelector = labelSelector.String()
		return c.Get().
			Namespace(namespace).
			Resource(resource).
			VersionedParams(&options, meta_v1.ParameterCodec).
			Watch()
	}
	return &cache.ListWatch{ListFunc: listFunc, WatchFunc: watchFunc}
}

// Reconcile handles enqueued messages
func (c *SnapshotControllerImpl) Reconcile(u *v1.Snapshot) error {

	err := c.reconcile(u)
	if err != nil {
		glog.Errorf("Reconcile: %v\n", err)
	}
	return err
}

func (c *SnapshotControllerImpl) getNode(pv *apiv1.PersistentVolume) (string, error) {
	nodesClient := c.kubeClient.CoreV1Client.Nodes()
	list, err := nodesClient.List(meta_v1.ListOptions{})
	if err != nil {
		return "", err
	}

	uniqueName := fmt.Sprintf("virtuozzo/ploop/%s",
		pv.Spec.PersistentVolumeSource.FlexVolume.Options["volumeID"])
	var node *apiv1.Node = nil
	for _, d := range list.Items {
		fmt.Printf(" * %s %s\n", d.Name, d.Namespace)
		for _, v := range d.Status.VolumesInUse {
			fmt.Printf("\t * %s %s %s %s\n", d.Name, d.Namespace, string(v), uniqueName)
			if string(v) == uniqueName {
				node = &d
				break
			}
		}
	}

	var nodeName string
	if node == nil {
		glog.Infof("The %s volume isn't mounted\n", pv.Name)
	} else {
		glog.Infof("Node %s: Volume %s\n", node.Name, pv.Name)
		nodeName = node.Name
	}

	return nodeName, nil
}

func (c *SnapshotControllerImpl) schedulePod(cmd string, u *v1.Snapshot, node, imagePath string) (string, error) {
	podsClient := c.kubeClient.CoreV1Client.Pods(apiv1.NamespaceDefault)

	snapPath := fmt.Sprintf("%s/%s", u.Spec.Options["snapshotPath"], u.Spec.Options["snapshotID"])

	secretName := u.Spec.Options["secret"]
	secretClient := c.kubeClient.Core().Secrets(u.ObjectMeta.Namespace)
	secret, err := secretClient.Get(secretName, meta_v1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("Unable to get a secrete %s: %v", secretName, err)
	}

	cluster := string(secret.Data["clusterName"][:len(secret.Data["clusterName"])])
	clusterMount := fmt.Sprintf("/var/run/ploop-flexvol/%s", cluster)

	path := filepath.Clean(fmt.Sprintf("%s/%s", clusterMount, imagePath))
	ploopMount := fmt.Sprintf("/var/run/ploop-flexvol/mounts/ploop-%x/mnt",
		md5.Sum([]byte(path)))

	podName := fmt.Sprintf("snap-pod-%s", uuid.NewUUID())
	u.Status.PodName = podName
	_, err = c.client.Snapshots(u.ObjectMeta.Namespace).UpdateStatus(u)
	if err != nil {
		return "", err
	}
	c.pods[u.Status.PodName] = u

	privileged := true
	pod := &apiv1.Pod{
		ObjectMeta: meta_v1.ObjectMeta{
			Name:   podName,
			Labels: map[string]string{"service": "vzsnap"},
		},
		Spec: apiv1.PodSpec{
			HostNetwork: true,
			Containers: []apiv1.Container{
				{
					Name:  "vzsnap",
					Image: "docker.io/avagin/vzsnap-k8s-job",
					VolumeMounts: []apiv1.VolumeMount{
						{
							Name:      "auth",
							MountPath: "/etc/vstorage-auth",
							ReadOnly:  true,
						},
					},
					SecurityContext: &apiv1.SecurityContext{
						Privileged: &privileged,
					},
				},
			},
			Volumes: []apiv1.Volume{
				{
					Name: "auth",
					VolumeSource: apiv1.VolumeSource{
						Secret: &apiv1.SecretVolumeSource{
							SecretName: secretName,
						},
					},
				},
			},
			RestartPolicy: apiv1.RestartPolicyNever,
		},
	}

	if cmd == "create" {
		pod.Spec.Containers[0].Command = []string{"vzsnap-cmd", cmd, imagePath, snapPath}
	} else if cmd == "delete" {
		pod.Spec.Containers[0].Command = []string{"vzsnap-cmd", cmd, snapPath}
	} else {
		panic(cmd)
	}
	if node != "" {
		pod.Spec.NodeSelector = map[string]string{"kubernetes.io/hostname": node}

		pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts,
			apiv1.VolumeMount{
				Name:      "vstorage",
				MountPath: clusterMount,
			},
			apiv1.VolumeMount{
				Name:      "ploop-mount",
				MountPath: ploopMount,
			})
		pod.Spec.Volumes = append(pod.Spec.Volumes,
			apiv1.Volume{
				Name: "vstorage",
				VolumeSource: apiv1.VolumeSource{
					HostPath: &apiv1.HostPathVolumeSource{
						Path: clusterMount,
					},
				},
			},
			apiv1.Volume{
				Name: "ploop-mount",
				VolumeSource: apiv1.VolumeSource{
					HostPath: &apiv1.HostPathVolumeSource{
						Path: ploopMount,
					},
				},
			},
		)
	}

	glog.Infoln("Creating pod...")
	result, err := podsClient.Create(pod)
	if err != nil {
		return "", err
	}
	glog.Infof("Created pod %q.\n", result.GetObjectMeta().GetName())

	return podName, nil
}

func (c *SnapshotControllerImpl) scheduleSnapshot(u *v1.Snapshot) error {
	pvcClient := c.kubeClient.CoreV1Client.PersistentVolumeClaims(u.ObjectMeta.Namespace)

	pvc, err := pvcClient.Get(u.Spec.SnapshottedVolumeClaim, meta_v1.GetOptions{})
	if err != nil {
		return fmt.Errorf("Unable to find %s: %v", u.Spec.SnapshottedVolumeClaim, err)
	}
	if pvc.Status.Phase != apiv1.ClaimBound {
		return fmt.Errorf("%s isn't bound", u.Spec.SnapshottedVolumeClaim)
	}

	volumesClient := c.kubeClient.CoreV1Client.PersistentVolumes()
	pv, err := volumesClient.Get(pvc.Spec.VolumeName, meta_v1.GetOptions{})
	if err != nil {
		return fmt.Errorf("Unable to find the volume %s", pvc.Spec.VolumeName)
	}

	scClient := c.kubeClient.StorageV1Client.StorageClasses()
	sc, err := scClient.Get(pv.Spec.StorageClassName, meta_v1.GetOptions{})
	if err != nil {
		return fmt.Errorf("Unable to find the %s storage class: %v",
			pv.Spec.StorageClassName, err)
	}

	nodeName, err := c.getNode(pv)
	if err != nil {
		return err
	}

	imagePath := fmt.Sprintf("%s/%s",
		pv.Spec.PersistentVolumeSource.FlexVolume.Options["volumePath"],
		pv.Spec.PersistentVolumeSource.FlexVolume.Options["volumeID"])

	u.Spec.Options = make(map[string]string)
	share := fmt.Sprintf("vzsnap-%s", uuid.NewUUID())
	snapPath, ok := sc.Parameters["snapshotPath"]
	if !ok {
		snapPath = sc.Parameters["volumePath"]
	}
	u.Spec.Options["volumeName"] = pv.Name
	u.Spec.Options["snapshotPath"] = snapPath
	u.Spec.Options["snapshotID"] = share
	u.Spec.Options["secret"] = pv.Spec.PersistentVolumeSource.FlexVolume.SecretRef.Name

	u, err = c.client.Snapshots(u.ObjectMeta.Namespace).Update(u)
	if err != nil {
		return err
	}

	u.Status.Phase = v1.SnapshotScheduled
	_, err = c.schedulePod("create", u, nodeName, imagePath)
	if err != nil {
		return err
	}

	return nil
}

func (c *SnapshotControllerImpl) delete(u *v1.Snapshot) error {
	glog.Infof("Delete %s\n", u.Name)

	volumeName := u.Spec.Options["volumeName"]

	volumesClient := c.kubeClient.CoreV1Client.PersistentVolumes()
	pv, err := volumesClient.Get(volumeName, meta_v1.GetOptions{})
	if err != nil {
		return fmt.Errorf("Unable to find the volume %s", volumeName)
	}

	nodeName, err := c.getNode(pv)
	if err != nil {
		return err
	}

	imagePath := fmt.Sprintf("%s/%s",
		pv.Spec.PersistentVolumeSource.FlexVolume.Options["volumePath"],
		pv.Spec.PersistentVolumeSource.FlexVolume.Options["volumeID"])

	u.Status.Phase = v1.SnapshotTerminating
	_, err = c.schedulePod("delete", u, nodeName, imagePath)
	if err != nil {
		return err
	}

	return nil
}

func (c *SnapshotControllerImpl) reconcile(u *v1.Snapshot) error {
	// Implement controller logic here
	glog.Infof("Running reconcile Snapshot for %s\n", u.Name)

	if u.DeletionTimestamp != nil {
		if u.Status.Phase == v1.SnapshotFailed {
			return nil
		}
		if u.Status.Phase == v1.SnapshotAvailable {
			return c.delete(u)
		}
		glog.Infof("Unable to delete the %s snapshot (%s)", u.Name, u.Status.Phase)
		return nil
	}

	if u.Status.Phase == v1.SnapshotPending {
		return c.scheduleSnapshot(u)
	}

	if u.Status.Phase == v1.SnapshotScheduled {
		c.pods[u.Status.PodName] = u
		return nil
	}

	return nil
}

func (c *SnapshotControllerImpl) Get(namespace, name string) (*v1.Snapshot, error) {
	return c.lister.Snapshots(namespace).Get(name)
}
