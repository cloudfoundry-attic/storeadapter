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

	itRetries := func(action func() error, resultIn func(error), attempts func() int) {
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

					retryPolicy.DelayForStub = func(callCount int) (time.Duration, bool) {
						Ω(attempts()).Should(Equal(callCount + 1))

						select {
						case d, ok := <-durations:
							return d, ok
						}
					}
				})

				It("continuously retries with an increasing attempt count", func() {
					Ω(retryPolicy.DelayForCallCount()).Should(Equal(4))
					Ω(sleeper.SleepCallCount()).Should(Equal(3))

					Ω(retryPolicy.DelayForArgsForCall(0)).Should(Equal(0))
					Ω(sleeper.SleepArgsForCall(0)).Should(Equal(time.Second))

					Ω(retryPolicy.DelayForArgsForCall(1)).Should(Equal(1))
					Ω(sleeper.SleepArgsForCall(1)).Should(Equal(2 * time.Second))

					Ω(retryPolicy.DelayForArgsForCall(2)).Should(Equal(2))
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

			It("does not error", func() {
				Ω(errResult).ShouldNot(HaveOccurred())
			})
		})
	}

	Describe("Create", func() {
		itRetries(func() error {
			return adapter.Create(StoreNode{})
		}, func(err error) {
			innerStoreAdapter.CreateReturns(err)
		}, func() int {
			return innerStoreAdapter.CreateCallCount()
		})
	})

	Describe("Update", func() {
		itRetries(func() error {
			return adapter.Update(StoreNode{})
		}, func(err error) {
			innerStoreAdapter.UpdateReturns(err)
		}, func() int {
			return innerStoreAdapter.UpdateCallCount()
		})
	})

	Describe("CompareAndSwap", func() {
		itRetries(func() error {
			return adapter.CompareAndSwap(StoreNode{}, StoreNode{})
		}, func(err error) {
			innerStoreAdapter.CompareAndSwapReturns(err)
		}, func() int {
			return innerStoreAdapter.CompareAndSwapCallCount()
		})
	})

	Describe("CompareAndSwapByIndex", func() {
		itRetries(func() error {
			return adapter.CompareAndSwapByIndex(123, StoreNode{})
		}, func(err error) {
			innerStoreAdapter.CompareAndSwapByIndexReturns(err)
		}, func() int {
			return innerStoreAdapter.CompareAndSwapByIndexCallCount()
		})
	})

	Describe("SetMulti", func() {
		itRetries(func() error {
			return adapter.SetMulti([]StoreNode{})
		}, func(err error) {
			innerStoreAdapter.SetMultiReturns(err)
		}, func() int {
			return innerStoreAdapter.SetMultiCallCount()
		})
	})

	// Describe("Get", func() {
	// 	itRetries(func() error {
	// 		_, err := adapter.Get("lol")
	// 		return err
	// 	}, func(err error) {
	// 		innerStoreAdapter.GetReturns(StoreNode{}, err)
	// 	}, func() int {
	// 		return innerStoreAdapter.GetCallCount()
	// 	})
	// })

	// Describe("ListRecursively", func() {
	//
	// })

	Describe("Delete", func() {
		itRetries(func() error {
			return adapter.Delete("lol")
		}, func(err error) {
			innerStoreAdapter.DeleteReturns(err)
		}, func() int {
			return innerStoreAdapter.DeleteCallCount()
		})
	})

	Describe("DeleteLeaves", func() {

	})

	Describe("CompareAndDelete", func() {

	})

	Describe("CompareAndDeleteByIndex", func() {

	})

	Describe("UpdateDirTTL", func() {

	})
})
