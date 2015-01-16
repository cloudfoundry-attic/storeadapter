package storeadapter_test

import (
	"errors"
	"time"

	. "github.com/cloudfoundry/storeadapter"
	"github.com/cloudfoundry/storeadapter/fakes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Retryable", func() {
	var (
		innerStoreAdapter *fakes.FakeStoreAdapter
		retryPolicy       *fakes.FakeRetryPolicy
		sleeper           *fakes.FakeSleeper

		adapter StoreAdapter
	)

	BeforeEach(func() {
		innerStoreAdapter = new(fakes.FakeStoreAdapter)
		retryPolicy = new(fakes.FakeRetryPolicy)
		sleeper = new(fakes.FakeSleeper)

		adapter = NewRetryable(innerStoreAdapter, sleeper, retryPolicy)
	})

	itRetries := func(action func() error, resultIn func(error), attempts func() int, example func()) {
		var errResult error

		JustBeforeEach(func() {
			errResult = action()
		})

		Context("when the store adapter returns a timeout error", func() {
			BeforeEach(func() {
				resultIn(ErrorTimeout)
			})

			Context("as long as the backoff policy returns true", func() {
				BeforeEach(func() {
					durations := make(chan time.Duration, 3)
					durations <- time.Second
					durations <- 2 * time.Second
					durations <- 1000 * time.Second
					close(durations)

					retryPolicy.DelayForStub = func(failedAttempts uint) (time.Duration, bool) {
						Ω(attempts()).Should(Equal(int(failedAttempts)))

						select {
						case d, ok := <-durations:
							return d, ok
						}
					}
				})

				It("continuously retries with an increasing attempt count", func() {
					Ω(retryPolicy.DelayForCallCount()).Should(Equal(4))
					Ω(sleeper.SleepCallCount()).Should(Equal(3))

					Ω(retryPolicy.DelayForArgsForCall(0)).Should(Equal(uint(1)))
					Ω(sleeper.SleepArgsForCall(0)).Should(Equal(time.Second))

					Ω(retryPolicy.DelayForArgsForCall(1)).Should(Equal(uint(2)))
					Ω(sleeper.SleepArgsForCall(1)).Should(Equal(2 * time.Second))

					Ω(retryPolicy.DelayForArgsForCall(2)).Should(Equal(uint(3)))
					Ω(sleeper.SleepArgsForCall(2)).Should(Equal(1000 * time.Second))

					Ω(errResult).Should(Equal(ErrorTimeout))
				})
			})
		})

		Context("when the store adapter returns a non-timeout error", func() {
			var adapterErr error

			BeforeEach(func() {
				adapterErr = errors.New("oh no!")
				resultIn(adapterErr)
			})

			It("propagates the error", func() {
				Ω(errResult).Should(Equal(adapterErr))
			})
		})

		Context("when the store adapter succeeds", func() {
			BeforeEach(func() {
				resultIn(nil)
			})

			example()

			It("does not error", func() {
				Ω(errResult).ShouldNot(HaveOccurred())
			})
		})
	}

	Describe("Create", func() {
		createdNode := StoreNode{
			Key:   "created-key",
			Value: []byte("created-value"),
		}

		itRetries(func() error {
			return adapter.Create(createdNode)
		}, func(err error) {
			innerStoreAdapter.CreateReturns(err)
		}, func() int {
			return innerStoreAdapter.CreateCallCount()
		}, func() {
			It("passes the node through", func() {
				Ω(innerStoreAdapter.CreateArgsForCall(0)).Should(Equal(createdNode))
			})
		})
	})

	Describe("Update", func() {
		updatedNode := StoreNode{
			Key:   "updated-key",
			Value: []byte("updated-value"),
		}

		itRetries(func() error {
			return adapter.Update(updatedNode)
		}, func(err error) {
			innerStoreAdapter.UpdateReturns(err)
		}, func() int {
			return innerStoreAdapter.UpdateCallCount()
		}, func() {
			It("passes the node through", func() {
				Ω(innerStoreAdapter.UpdateArgsForCall(0)).Should(Equal(updatedNode))
			})
		})
	})

	Describe("CompareAndSwap", func() {
		oldNode := StoreNode{
			Key:   "old-key",
			Value: []byte("old-value"),
		}

		newNode := StoreNode{
			Key:   "new-key",
			Value: []byte("new-value"),
		}

		itRetries(func() error {
			return adapter.CompareAndSwap(oldNode, newNode)
		}, func(err error) {
			innerStoreAdapter.CompareAndSwapReturns(err)
		}, func() int {
			return innerStoreAdapter.CompareAndSwapCallCount()
		}, func() {
			It("passes the nodes through", func() {
				oldN, newN := innerStoreAdapter.CompareAndSwapArgsForCall(0)
				Ω(oldN).Should(Equal(oldNode))
				Ω(newN).Should(Equal(newNode))
			})
		})
	})

	Describe("CompareAndSwapByIndex", func() {
		var comparedIndex uint64 = 123
		swappedNode := StoreNode{
			Key:   "swapped-key",
			Value: []byte("swapped-value"),
		}

		itRetries(func() error {
			return adapter.CompareAndSwapByIndex(comparedIndex, swappedNode)
		}, func(err error) {
			innerStoreAdapter.CompareAndSwapByIndexReturns(err)
		}, func() int {
			return innerStoreAdapter.CompareAndSwapByIndexCallCount()
		}, func() {
			It("passes the node and index through", func() {
				index, node := innerStoreAdapter.CompareAndSwapByIndexArgsForCall(0)
				Ω(index).Should(Equal(uint64(comparedIndex)))
				Ω(node).Should(Equal(swappedNode))
			})
		})
	})

	Describe("SetMulti", func() {
		nodes := []StoreNode{
			{Key: "key-a", Value: []byte("value-a")},
			{Key: "key-b", Value: []byte("value-b")},
		}

		itRetries(func() error {
			return adapter.SetMulti(nodes)
		}, func(err error) {
			innerStoreAdapter.SetMultiReturns(err)
		}, func() int {
			return innerStoreAdapter.SetMultiCallCount()
		}, func() {
			It("passes the nodes through", func() {
				Ω(innerStoreAdapter.SetMultiArgsForCall(0)).Should(Equal(nodes))
			})
		})
	})

	Describe("Get", func() {
		nodeToReturn := StoreNode{
			Key:   "returned-key",
			Value: []byte("returned-value"),
		}

		var gotNode StoreNode

		itRetries(func() error {
			var err error

			gotNode, err = adapter.Get("getting-key")
			return err
		}, func(err error) {
			innerStoreAdapter.GetReturns(nodeToReturn, err)
		}, func() int {
			return innerStoreAdapter.GetCallCount()
		}, func() {
			It("passes the key through", func() {
				Ω(innerStoreAdapter.GetArgsForCall(0)).Should(Equal("getting-key"))
			})

			It("returns the node", func() {
				Ω(gotNode).Should(Equal(nodeToReturn))
			})
		})
	})

	Describe("ListRecursively", func() {
		nodeToReturn := StoreNode{
			Key:   "returned-key",
			Value: []byte("returned-value"),
		}

		var listedNode StoreNode

		itRetries(func() error {
			var err error

			listedNode, err = adapter.ListRecursively("listing-key")
			return err
		}, func(err error) {
			innerStoreAdapter.ListRecursivelyReturns(nodeToReturn, err)
		}, func() int {
			return innerStoreAdapter.ListRecursivelyCallCount()
		}, func() {
			It("passes the key through", func() {
				Ω(innerStoreAdapter.ListRecursivelyArgsForCall(0)).Should(Equal("listing-key"))
			})

			It("returns the node", func() {
				Ω(listedNode).Should(Equal(nodeToReturn))
			})
		})
	})

	Describe("Delete", func() {
		keysToDelete := []string{"key1", "key2"}

		itRetries(func() error {
			return adapter.Delete(keysToDelete...)
		}, func(err error) {
			innerStoreAdapter.DeleteReturns(err)
		}, func() int {
			return innerStoreAdapter.DeleteCallCount()
		}, func() {
			It("passes the keys through", func() {
				Ω(innerStoreAdapter.DeleteArgsForCall(0)).Should(Equal(keysToDelete))
			})
		})
	})

	Describe("DeleteLeaves", func() {
		keysToDelete := []string{"key1", "key2"}

		itRetries(func() error {
			return adapter.DeleteLeaves(keysToDelete...)
		}, func(err error) {
			innerStoreAdapter.DeleteLeavesReturns(err)
		}, func() int {
			return innerStoreAdapter.DeleteLeavesCallCount()
		}, func() {
			It("passes the keys through", func() {
				Ω(innerStoreAdapter.DeleteLeavesArgsForCall(0)).Should(Equal(keysToDelete))
			})
		})
	})

	Describe("CompareAndDelete", func() {
		nodesToCAD := []StoreNode{
			{Key: "key-a", Value: []byte("value-a")},
			{Key: "key-b", Value: []byte("value-b")},
		}

		itRetries(func() error {
			return adapter.CompareAndDelete(nodesToCAD...)
		}, func(err error) {
			innerStoreAdapter.CompareAndDeleteReturns(err)
		}, func() int {
			return innerStoreAdapter.CompareAndDeleteCallCount()
		}, func() {
			It("passes the node through", func() {
				nodes := innerStoreAdapter.CompareAndDeleteArgsForCall(0)
				Ω(nodes).Should(Equal(nodesToCAD))
			})
		})
	})

	Describe("CompareAndDeleteByIndex", func() {
		nodesToCAD := []StoreNode{
			{Key: "key-a", Value: []byte("value-a")},
			{Key: "key-b", Value: []byte("value-b")},
		}

		itRetries(func() error {
			return adapter.CompareAndDeleteByIndex(nodesToCAD...)
		}, func(err error) {
			innerStoreAdapter.CompareAndDeleteByIndexReturns(err)
		}, func() int {
			return innerStoreAdapter.CompareAndDeleteByIndexCallCount()
		}, func() {
			It("passes the node and index through", func() {
				nodes := innerStoreAdapter.CompareAndDeleteByIndexArgsForCall(0)
				Ω(nodes).Should(Equal(nodesToCAD))
			})
		})
	})

	Describe("UpdateDirTTL", func() {
		dirKey := "dir-key"
		var ttlToSet uint64 = 42

		itRetries(func() error {
			return adapter.UpdateDirTTL(dirKey, ttlToSet)
		}, func(err error) {
			innerStoreAdapter.UpdateDirTTLReturns(err)
		}, func() int {
			return innerStoreAdapter.UpdateDirTTLCallCount()
		}, func() {
			It("passes the keys through", func() {
				dir, ttl := innerStoreAdapter.UpdateDirTTLArgsForCall(0)
				Ω(dir).Should(Equal(dirKey))
				Ω(ttl).Should(Equal(ttlToSet))
			})
		})
	})
})
