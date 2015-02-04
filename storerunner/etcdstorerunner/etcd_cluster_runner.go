package etcdstorerunner

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/cloudfoundry/gunk/workpool"
	"github.com/cloudfoundry/storeadapter"
	"github.com/cloudfoundry/storeadapter/etcdstoreadapter"
	etcdclient "github.com/coreos/go-etcd/etcd"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
	"github.com/pivotal-golang/clock"
)

type ETCDClusterRunner struct {
	startingPort int
	numNodes     int
	etcdSessions []*gexec.Session
	running      bool
	client       *etcdclient.Client

	mutex *sync.RWMutex
}

func NewETCDClusterRunner(startingPort int, numNodes int) *ETCDClusterRunner {
	return &ETCDClusterRunner{
		startingPort: startingPort,
		numNodes:     numNodes,

		mutex: &sync.RWMutex{},
	}
}

func (etcd *ETCDClusterRunner) Start() {
	etcd.start(true)
}

func (etcd *ETCDClusterRunner) Stop() {
	etcd.stop(true)
}

func (etcd *ETCDClusterRunner) KillWithFire() {
	etcd.kill()
}

func (etcd *ETCDClusterRunner) GoAway() {
	etcd.stop(false)
}

func (etcd *ETCDClusterRunner) ComeBack() {
	etcd.start(false)
}

func (etcd *ETCDClusterRunner) NodeURLS() []string {
	urls := make([]string, etcd.numNodes)
	for i := 0; i < etcd.numNodes; i++ {
		urls[i] = etcd.clientURL(i)
	}
	return urls
}

func (etcd *ETCDClusterRunner) DiskUsage() (bytes int64, err error) {
	fi, err := os.Stat(etcd.tmpPathTo("log", 0))
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

func (etcd *ETCDClusterRunner) Reset() {
	etcd.mutex.RLock()
	running := etcd.running
	etcd.mutex.RUnlock()

	if running {
		response, err := etcd.client.Get("/", false, false)
		if err == nil {
			for _, doomed := range response.Node.Nodes {
				etcd.client.Delete(doomed.Key, true)
			}
		}
	}
}

func (etcd *ETCDClusterRunner) FastForwardTime(seconds int) {
	etcd.mutex.RLock()
	running := etcd.running
	etcd.mutex.RUnlock()

	if running {
		response, err := etcd.client.Get("/", false, true)
		Ω(err).ShouldNot(HaveOccurred())
		etcd.fastForwardTime(response.Node, seconds)
	}
}

func (etcd *ETCDClusterRunner) Adapter() storeadapter.StoreAdapter {
	pool := workpool.NewWorkPool(10)
	adapter := etcdstoreadapter.NewETCDStoreAdapter(etcd.NodeURLS(), pool)
	adapter.Connect()
	return adapter
}

func (etcd *ETCDClusterRunner) RetryableAdapter() storeadapter.StoreAdapter {
	pool := workpool.NewWorkPool(10)

	adapter := storeadapter.NewRetryable(
		etcdstoreadapter.NewETCDStoreAdapter(etcd.NodeURLS(), pool),
		clock.NewClock(),
		storeadapter.ExponentialRetryPolicy{},
	)

	adapter.Connect()

	return adapter
}

func (etcd *ETCDClusterRunner) start(nuke bool) {
	etcd.mutex.RLock()
	running := etcd.running
	etcd.mutex.RUnlock()

	if running {
		return
	}

	etcd.mutex.Lock()
	defer etcd.mutex.Unlock()

	etcd.etcdSessions = make([]*gexec.Session, etcd.numNodes)

	clusterURLs := make([]string, etcd.numNodes)
	for i := 0; i < etcd.numNodes; i++ {
		clusterURLs[i] = etcd.nodeName(i) + "=" + etcd.serverURL(i)
	}

	for i := 0; i < etcd.numNodes; i++ {
		if nuke {
			etcd.nukeArtifacts(i)
		}

		if etcd.detectRunningEtcd(i) {
			log.Fatalf("Detected an ETCD already running on %s", etcd.clientURL(i))
		}

		os.MkdirAll(etcd.tmpPath(i), 0700)
		args := []string{
			"--name", etcd.nodeName(i),
			"--data-dir", etcd.tmpPath(i),
			"--listen-client-urls", etcd.clientURL(i),
			"--listen-peer-urls", etcd.serverURL(i),
			"--initial-cluster", strings.Join(clusterURLs, ","),
			"--initial-advertise-peer-urls", etcd.serverURL(i),
			"--initial-cluster-state", "new",
			"--advertise-client-urls", etcd.clientURL(i),
		}

		session, err := gexec.Start(
			exec.Command("etcd", args...),
			gexec.NewPrefixedWriter("\x1b[32m[o]\x1b[33m[etcd_cluster]\x1b[0m ", ginkgo.GinkgoWriter),
			gexec.NewPrefixedWriter("\x1b[91m[e]\x1b[33m[etcd_cluster]\x1b[0m ", ginkgo.GinkgoWriter),
		)
		Ω(err).ShouldNot(HaveOccurred(), "Make sure etcd is compiled and on your $PATH.")

		etcd.etcdSessions[i] = session

		Eventually(func() bool {
			defer func() {
				// https://github.com/coreos/go-etcd/issues/114
				recover()
			}()

			return etcd.detectRunningEtcd(i)
		}, 10, 0.05).Should(BeTrue(), "Expected ETCD to be up and running")
	}

	etcd.client = etcdclient.NewClient(etcd.NodeURLS())
	etcd.running = true
}

func (etcd *ETCDClusterRunner) stop(nuke bool) {
	etcd.mutex.Lock()
	defer etcd.mutex.Unlock()

	if etcd.running {
		for i := 0; i < etcd.numNodes; i++ {
			etcd.etcdSessions[i].Interrupt().Wait(5 * time.Second)
			if nuke {
				etcd.nukeArtifacts(i)
			}
		}
		etcd.markAsStopped()
	}
}

func (etcd *ETCDClusterRunner) kill() {
	etcd.mutex.Lock()
	defer etcd.mutex.Unlock()

	if etcd.running {
		for i := 0; i < etcd.numNodes; i++ {
			etcd.etcdSessions[i].Kill().Wait(5 * time.Second)
			etcd.nukeArtifacts(i)
		}
		etcd.markAsStopped()
	}
}

func (etcd *ETCDClusterRunner) markAsStopped() {
	etcd.etcdSessions = nil
	etcd.running = false
	etcd.client = nil
}

func (etcd *ETCDClusterRunner) detectRunningEtcd(index int) bool {
	client := etcdclient.NewClient([]string{})
	return client.SetCluster([]string{etcd.clientURL(index)})
}

func (etcd *ETCDClusterRunner) fastForwardTime(etcdNode *etcdclient.Node, seconds int) {
	if etcdNode.Dir == true {
		for _, child := range etcdNode.Nodes {
			etcd.fastForwardTime(child, seconds)
		}
	} else {
		if etcdNode.TTL == 0 {
			return
		}
		if etcdNode.TTL <= int64(seconds) {
			_, err := etcd.client.Delete(etcdNode.Key, true)
			Ω(err).ShouldNot(HaveOccurred())
		} else {
			_, err := etcd.client.Set(etcdNode.Key, etcdNode.Value, uint64(etcdNode.TTL-int64(seconds)))
			Ω(err).ShouldNot(HaveOccurred())
		}
	}
}

func (etcd *ETCDClusterRunner) clientURL(index int) string {
	return fmt.Sprintf("http://127.0.0.1:%d", etcd.port(index))
}

func (etcd *ETCDClusterRunner) serverURL(index int) string {
	return fmt.Sprintf("http://127.0.0.1:%d", etcd.port(index)+3000)
}

func (etcd *ETCDClusterRunner) nodeName(index int) string {
	return fmt.Sprintf("node%d", index)
}

func (etcd *ETCDClusterRunner) port(index int) int {
	return etcd.startingPort + index
}

func (etcd *ETCDClusterRunner) tmpPath(index int) string {
	return fmt.Sprintf("/tmp/ETCD_%d", etcd.port(index))
}

func (etcd *ETCDClusterRunner) tmpPathTo(subdir string, index int) string {
	return fmt.Sprintf("/%s/%s", etcd.tmpPath(index), subdir)
}

func (etcd *ETCDClusterRunner) nukeArtifacts(index int) {
	os.RemoveAll(etcd.tmpPath(index))
}
