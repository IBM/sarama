package cluster

import (
	"time"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("GUID", func() {

	BeforeEach(func() {
		GUID.hostname = "testhost"
		GUID.pid = 20100
	})

	AfterEach(func() {
		GUID.inc = 0
	})

	It("should create GUIDs", func() {
		GUID.inc = 0xffffffff
		Expect(GUID.NewAt("prefix", time.Unix(1313131313, 0))).To(Equal("prefix-testhost-20100-1313131313-0"))
		Expect(GUID.NewAt("prefix", time.Unix(1414141414, 0))).To(Equal("prefix-testhost-20100-1414141414-1"))
	})

	It("should increment correctly", func() {
		GUID.inc = 0xffffffff - 1
		Expect(GUID.NewAt("prefix", time.Unix(1313131313, 0))).To(Equal("prefix-testhost-20100-1313131313-4294967295"))
		Expect(GUID.NewAt("prefix", time.Unix(1313131313, 0))).To(Equal("prefix-testhost-20100-1313131313-0"))
	})
})
