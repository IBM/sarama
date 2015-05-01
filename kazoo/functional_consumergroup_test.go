package kazoo

import (
	"testing"
	"time"
)

func TestConsumergroups(t *testing.T) {
	kz, err := NewKazoo(zookeeperPeers, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer assertSuccessfulClose(t, kz)

	cg := kz.Consumergroup("test.kazoo.TestConsumergroups")

	cgs, err := kz.Consumergroups()
	if err != nil {
		t.Error(err)
	}
	originalCount := len(cgs)

	if _, ok := cgs[cg.Name]; ok {
		t.Error("Consumergoup `test.kazoo.TestConsumergroups` should not be returned")
	}

	if exists, _ := cg.Exists(); exists {
		t.Error("Consumergoup `test.kazoo.TestConsumergroups` should not be registered yet")
	}

	if err := cg.Create(); err != nil {
		t.Error(err)
	}

	if exists, _ := cg.Exists(); !exists {
		t.Error("Consumergoup `test.kazoo.TestConsumergroups` should be registered now")
	}

	cgs, err = kz.Consumergroups()
	if err != nil {
		t.Error(err)
	}

	if len(cgs) != originalCount+1 {
		t.Error("Should have one more consumergroup than at the start")
	}

	if err := cg.Delete(); err != nil {
		t.Error(err)
	}

	if exists, _ := cg.Exists(); exists {
		t.Error("Consumergoup `test.kazoo.TestConsumergroups` should not be registered anymore")
	}

	cgs, err = kz.Consumergroups()
	if err != nil {
		t.Error(err)
	}

	if len(cgs) != originalCount {
		t.Error("Should have the original number of consumergroups again")
	}
}

func TestConsumergroupInstances(t *testing.T) {
	kz, err := NewKazoo(zookeeperPeers, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer assertSuccessfulClose(t, kz)

	cg := kz.Consumergroup("test.kazoo.TestConsumergroupInstances")
	if err := cg.Create(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cg.Delete(); err != nil {
			t.Error(err)
		}
	}()

	instances, err := cg.Instances()
	if err != nil {
		t.Error(err)
	}
	if len(instances) != 0 {
		t.Error("Expected no active consumergroup instances")
	}

	// Register a new instance
	instance1 := cg.NewInstance()
	if instance1.ID == "" {
		t.Error("It should generate a valid instance ID")
	}
	if err := instance1.Register([]string{"topic"}); err != nil {
		t.Error(err)
	}

	// Try to register an instance with the same ID.
	if err := cg.Instance(instance1.ID).Register([]string{"topic"}); err != ErrInstanceAlreadyRegistered {
		t.Error("The instance should already be registered")
	}

	instance2 := cg.Instance("test")
	if err := instance2.Register([]string{"topic"}); err != nil {
		t.Error(err)
	}

	instances, err = cg.Instances()
	if err != nil {
		t.Error(err)
	}
	if len(instances) != 2 {
		t.Error("Expected 2 active consumergroup instances")
	}
	if _, ok := instances[instance1.ID]; !ok {
		t.Error("Expected instance1 to be registered.")
	}
	if _, ok := instances[instance1.ID]; !ok {
		t.Error("Expected instance2 to be registered.")
	}

	// Deregister the two running instances
	if err := instance1.Deregister(); err != nil {
		t.Error(err)
	}
	if err := instance2.Deregister(); err != nil {
		t.Error(err)
	}

	// Try to deregister an instance that was not register
	instance3 := cg.NewInstance()
	if err := instance3.Deregister(); err != ErrInstanceNotRegistered {
		t.Error("Expected new instance to not be registered")
	}

	instances, err = cg.Instances()
	if err != nil {
		t.Error(err)
	}
	if len(instances) != 0 {
		t.Error("Expected no active consumergroup instances")
	}
}

func TestConsumergroupInstanceCrash(t *testing.T) {
	kz, err := NewKazoo(zookeeperPeers, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer assertSuccessfulClose(t, kz)

	cg := kz.Consumergroup("test.kazoo.TestConsumergroupInstancesEphemeral")
	if err := cg.Create(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cg.Delete(); err != nil {
			t.Error(err)
		}
	}()

	// Create a kazoo instance on which we will simulate a crash.
	config := NewConfig()
	config.Timeout = 10 * time.Millisecond
	crashingKazoo, err := NewKazoo(zookeeperPeers, config)
	if err != nil {
		t.Fatal(err)
	}
	crashingCG := crashingKazoo.Consumergroup(cg.Name)

	// Instantiate and register the instance.
	instance := crashingCG.NewInstance()
	if err := instance.Register([]string{"test.1"}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(10 * time.Millisecond)

	if instances, err := cg.Instances(); err != nil {
		t.Error(err)
	} else if len(instances) != 1 {
		t.Error("Should have 1 running instance, found", len(instances))
	}

	// Simulate a crash, and wait for Zookeeper to pick it up
	_ = crashingKazoo.Close()
	time.Sleep(50 * time.Millisecond)

	if instances, err := cg.Instances(); err != nil {
		t.Error(err)
	} else if len(instances) != 0 {
		t.Error("Should have 0 running instances, found", len(instances))
	}
}

func TestConsumergroupOffsets(t *testing.T) {
	kz, err := NewKazoo(zookeeperPeers, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer assertSuccessfulClose(t, kz)

	cg := kz.Consumergroup("test.kazoo.TestConsumergroupOffsets")
	if err := cg.Create(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cg.Delete(); err != nil {
			t.Error(err)
		}
	}()

	offset, err := cg.FetchOffset("test", 0)
	if err != nil {
		t.Error(err)
	}

	if offset != 0 {
		t.Error("Expected to get offset 0 for a partition that hasn't seen an offset commit yet")
	}

	if err := cg.CommitOffset("test", 0, 1234); err != nil {
		t.Error(err)
	}

	offset, err = cg.FetchOffset("test", 0)
	if err != nil {
		t.Error(err)
	}
	if offset != 1234 {
		t.Error("Expected to get the offset that was committed.")
	}
}
