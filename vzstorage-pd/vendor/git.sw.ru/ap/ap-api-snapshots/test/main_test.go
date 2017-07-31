package main

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"git.sw.ru/ap/ap-api-snapshots/pkg/apis/snapshots/v1"
	"git.sw.ru/ap/ap-api-snapshots/pkg/client/clientset_generated/clientset"
	snapshotsV1 "git.sw.ru/ap/ap-api-snapshots/pkg/client/clientset_generated/clientset/typed/snapshots/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/clientcmd"
)

func snapshtoAvaliable(c *snapshotsV1.SnapshotsV1Client, snapName, namespace string) wait.ConditionFunc {
	return func() (bool, error) {
		snap, err := c.Snapshots(namespace).Get(snapName, meta_v1.GetOptions{})
		if err != nil {
			return false, err
		}
		switch snap.Status.Phase {
		case v1.SnapshotAvailable:
			return true, nil
		case v1.SnapshotFailed:
			return false, fmt.Errorf("v1.SnapshotFailed")
		}
		return false, nil
	}
}

func snapshtoDeleted(t *testing.T, c *snapshotsV1.SnapshotsV1Client, snapName, namespace string) wait.ConditionFunc {
	return func() (bool, error) {
		snap, err := c.Snapshots(namespace).Get(snapName, meta_v1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			t.Fatalf("Unable to get snapshot: %v\n", err)
		}
		switch snap.Status.Phase {
		case v1.SnapshotFailed:
			return false, fmt.Errorf("v1.SnapshotFailed")
		}
		return false, nil
	}
}

func TestTimeConsuming(t *testing.T) {
	config, err := clientcmd.BuildConfigFromFlags("", "../kubeconfig")
	if err != nil {
		t.Fatalf("%v", err)
	}
	cs := clientset.NewForConfigOrDie(config)
	client := cs.SnapshotsV1Client
	value := strconv.Itoa(time.Now().Nanosecond())

	name := "snap-submit-remove-" + string(uuid.NewUUID())
	t.Logf("Create %s\n", name)
	snap := &v1.Snapshot{
		ObjectMeta: meta_v1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"name": "foo",
				"time": value,
			},
		},
		Spec: v1.SnapshotSpec{
			SnapshottedVolumeClaim: "vz-test-claim",
		},
	}
	selector := labels.SelectorFromSet(labels.Set(map[string]string{"time": value}))
	options := meta_v1.ListOptions{LabelSelector: selector.String()}
	w, err := client.Snapshots("default").Watch(options)
	if err != nil {
		t.Fatalf("Unable to watch snapshots: %v", err)
	}
	snap, err = client.Snapshots("default").Create(snap)
	if err != nil {
		t.Fatalf("Unable to create snapshot: %v", err)
	}

	select {
	case event, _ := <-w.ResultChan():
		if event.Type != watch.Added {
			t.Fatalf("Failed to observe snapshot creation: %v", event)
		}
	case <-time.After(5 * time.Minute):
		{
			t.Fatalf("Timeout while waiting for snapshot creation\n")
		}
	}

	found := false
	t.Logf("Collect snapshots...")
	list, err := client.Snapshots("").List(meta_v1.ListOptions{})
	if err != nil {
		t.Fatalf("Unable to list snapshots: %v\n", err)
	}

	for _, s := range list.Items {
		if s.Name == snap.Name {
			found = true
		}
	}
	if !found {
		t.Fatalf("Unable to find snapshot")
	}

	err = wait.PollImmediate(2*time.Second, 180*time.Second, snapshtoAvaliable(client, snap.Name, "default"))
	if err != nil {
		t.Fatalf("Unable to wait snapshot: %v", err)
	}

	t.Logf("Wait %s %s\n", name, snap.Name)
	err = client.Snapshots("default").Delete(snap.Name, &meta_v1.DeleteOptions{})
	if err != nil {
		t.Fatalf("Unable to delete snapshot: %v\n", err)
	}

	err = wait.PollImmediate(2*time.Second, 180*time.Second, snapshtoDeleted(t, client, snap.Name, "default"))
	if err != nil {
		t.Fatalf("Unable to waint snapshot: %v", err)
	}
}
