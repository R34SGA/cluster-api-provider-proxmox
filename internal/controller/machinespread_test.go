/*
Copyright 2023-2026 IONOS Cloud.

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

package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	infrav1 "github.com/ionos-cloud/cluster-api-provider-proxmox/api/v1alpha2"
	"github.com/ionos-cloud/cluster-api-provider-proxmox/pkg/kubernetes/ipam"
	"github.com/ionos-cloud/cluster-api-provider-proxmox/pkg/proxmox/proxmoxtest"
	"github.com/ionos-cloud/cluster-api-provider-proxmox/pkg/scope"
)

func newSpreadScheme(t *testing.T) *runtime.Scheme {
	scheme := runtime.NewScheme()
	require.NoError(t, clusterv1.AddToScheme(scheme))
	require.NoError(t, infrav1.AddToScheme(scheme))
	return scheme
}

// spreadMachine builds a MachineSet-owned worker machine of the given zone.
func spreadMachine(name, zone string, age time.Duration, now time.Time) *clusterv1.Machine {
	machine := &clusterv1.Machine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: metav1.NamespaceDefault,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(
				&clusterv1.MachineSet{ObjectMeta: metav1.ObjectMeta{Name: "workers"}},
				clusterv1.GroupVersion.WithKind("MachineSet"),
			)},
			CreationTimestamp: metav1.NewTime(now.Add(-age)),
		},
		Spec: clusterv1.MachineSpec{
			ClusterName:   "test",
			FailureDomain: zone,
		},
		Status: clusterv1.MachineStatus{
			NodeRef: clusterv1.MachineNodeReference{Name: "node-" + name},
		},
	}
	return machine
}

// spreadProxmoxMachine builds a ProxmoxMachine whose status records the given zone.
func spreadProxmoxMachine(name, zone string) *infrav1.ProxmoxMachine {
	return &infrav1.ProxmoxMachine{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: metav1.NamespaceDefault},
		Status:     infrav1.ProxmoxMachineStatus{FailureDomain: zone},
	}
}

func newSpreadReconciler(t *testing.T, azs []infrav1.AvailabilityZoneSpec, machines ...client.Object) (*ProxmoxClusterReconciler, *scope.ClusterScope) {
	scheme := newSpreadScheme(t)
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(machines...).Build()

	proxmoxCluster := &infrav1.ProxmoxCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: metav1.NamespaceDefault},
		Spec:       infrav1.ProxmoxClusterSpec{AvailabilityZones: azs},
	}
	clusterScope, err := scope.NewClusterScope(scope.ClusterScopeParams{
		Client:         kubeClient,
		Cluster:        &clusterv1.Cluster{},
		ProxmoxCluster: proxmoxCluster,
		ProxmoxClient:  proxmoxtest.NewMockClient(t),
		IPAMHelper:     &ipam.Helper{},
	})
	require.NoError(t, err)

	return &ProxmoxClusterReconciler{Client: kubeClient}, clusterScope
}

func machineAnnotation(t *testing.T, c client.Client, name string) (string, bool) {
	machine := &clusterv1.Machine{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: metav1.NamespaceDefault, Name: name}, machine))
	value, ok := machine.GetAnnotations()[clusterv1.DeleteMachineAnnotation]
	return value, ok
}

// requireVictims asserts that exactly the given machines carry the annotation.
func requireVictims(t *testing.T, c client.Client, victims ...string) {
	t.Helper()
	expected := make(map[string]bool, len(victims))
	for _, name := range victims {
		expected[name] = true
	}
	machines := &clusterv1.MachineList{}
	require.NoError(t, c.List(context.Background(), machines, client.InNamespace(metav1.NamespaceDefault)))
	for _, machine := range machines.Items {
		_, annotated := machine.GetAnnotations()[clusterv1.DeleteMachineAnnotation]
		require.Equal(t, expected[machine.GetName()], annotated,
			"unexpected annotation state for %s", machine.GetName())
	}
}

func zonalAZs() []infrav1.AvailabilityZoneSpec {
	return []infrav1.AvailabilityZoneSpec{
		{Name: "az1", Nodes: []string{"pve-az1"}},
		{Name: "az2", Nodes: []string{"pve-az2"}},
		{Name: "az3", Nodes: []string{"pve-az3"}},
	}
}

func TestReconcileZonalSpread(t *testing.T) {
	now := time.Now()

	t.Run("annotates every machine beyond its zone's skeleton", func(t *testing.T) {
		// Reproduces the 1/1/2 production failure: az3 holds two machines,
		// its extra machine is the designated sacrifice, and the machines
		// keeping az1/az2 alive stay unannotated.
		az1Worker := spreadMachine("workers-az1", "az1", 20*time.Minute, now)
		az2Worker := spreadMachine("workers-az2", "az2", 4*time.Minute, now)
		az3Old := spreadMachine("workers-az3-old", "az3", 45*time.Minute, now)
		az3New := spreadMachine("workers-az3-new", "az3", 4*time.Minute, now)

		r, clusterScope := newSpreadReconciler(t, zonalAZs(), az1Worker, az2Worker, az3Old, az3New)
		require.NoError(t, r.reconcileZonalSpread(context.Background(), clusterScope))

		requireVictims(t, r.Client, "workers-az3-new")
	})

	t.Run("annotates one machine per zone of a balanced multi-replica set", func(t *testing.T) {
		// The bulk scale-down scenario: 2/2/2 replicas scaling straight to 3.
		// CAPI picks all three victims in a single pass, so every extra
		// machine (one per zone) must carry the annotation beforehand, or
		// the removal could empty a zone while another keeps two machines.
		az1Old := spreadMachine("workers-az1-old", "az1", 74*time.Minute, now)
		az1New := spreadMachine("workers-az1-new", "az1", 5*time.Minute, now)
		az2Old := spreadMachine("workers-az2-old", "az2", 19*time.Minute, now)
		az2New := spreadMachine("workers-az2-new", "az2", 5*time.Minute, now)
		az3Old := spreadMachine("workers-az3-old", "az3", 11*time.Minute, now)
		az3New := spreadMachine("workers-az3-new", "az3", 5*time.Minute, now)

		r, clusterScope := newSpreadReconciler(t, zonalAZs(), az1Old, az1New, az2Old, az2New, az3Old, az3New)
		require.NoError(t, r.reconcileZonalSpread(context.Background(), clusterScope))

		requireVictims(t, r.Client, "workers-az1-new", "workers-az2-new", "workers-az3-new")
	})

	t.Run("moves a stale annotation when the balance shifts", func(t *testing.T) {
		// az1 machine still carries the annotation from an earlier balance;
		// the overload now sits on az3, the annotation must move.
		az1Worker := spreadMachine("workers-az1", "az1", 20*time.Minute, now)
		az3Old := spreadMachine("workers-az3-old", "az3", 45*time.Minute, now)
		az2Worker := spreadMachine("workers-az2", "az2", 4*time.Minute, now)
		az3New := spreadMachine("workers-az3-new", "az3", 4*time.Minute, now)
		az1Worker.Annotations = map[string]string{clusterv1.DeleteMachineAnnotation: "yes"}

		r, clusterScope := newSpreadReconciler(t, zonalAZs(), az1Worker, az2Worker, az3Old, az3New)
		require.NoError(t, r.reconcileZonalSpread(context.Background(), clusterScope))

		requireVictims(t, r.Client, "workers-az3-new")
	})

	t.Run("one machine per zone carries no annotation", func(t *testing.T) {
		az1Worker := spreadMachine("workers-az1", "az1", 20*time.Minute, now)
		az2Worker := spreadMachine("workers-az2", "az2", 15*time.Minute, now)
		az3Worker := spreadMachine("workers-az3", "az3", 10*time.Minute, now)

		r, clusterScope := newSpreadReconciler(t, zonalAZs(), az1Worker, az2Worker, az3Worker)
		require.NoError(t, r.reconcileZonalSpread(context.Background(), clusterScope))

		requireVictims(t, r.Client)
	})

	t.Run("clears stale annotations once every zone holds one machine", func(t *testing.T) {
		az3Machine := spreadMachine("workers-az3", "az3", 45*time.Minute, now)
		az3Machine.Annotations = map[string]string{clusterv1.DeleteMachineAnnotation: "yes"}
		az1Worker := spreadMachine("workers-az1", "az1", 20*time.Minute, now)
		az2Worker := spreadMachine("workers-az2", "az2", 15*time.Minute, now)

		r, clusterScope := newSpreadReconciler(t, zonalAZs(), az1Worker, az2Worker, az3Machine)
		require.NoError(t, r.reconcileZonalSpread(context.Background(), clusterScope))

		requireVictims(t, r.Client)
	})

	t.Run("does nothing without availability zones", func(t *testing.T) {
		az3Old := spreadMachine("workers-az3-old", "az3", 45*time.Minute, now)
		az3New := spreadMachine("workers-az3-new", "az3", 4*time.Minute, now)

		r, clusterScope := newSpreadReconciler(t, nil, az3Old, az3New)
		require.NoError(t, r.reconcileZonalSpread(context.Background(), clusterScope))

		requireVictims(t, r.Client)
	})

	t.Run("ignores machines without a machine set owner", func(t *testing.T) {
		cpOwner := metav1.OwnerReference{
			APIVersion: clusterv1.GroupVersion.String(),
			Kind:       "KubeadmControlPlane",
			Name:       "control-plane",
			Controller: new(true),
		}
		cpMachine := spreadMachine("control-plane-0", "az1", 45*time.Minute, now)
		cpMachine.OwnerReferences = []metav1.OwnerReference{cpOwner}
		// Keep the control plane overloaded on paper: it must never be
		// annotated because KCP owns it, not a MachineSet.
		cpMachine2 := spreadMachine("control-plane-1", "az1", 30*time.Minute, now)
		cpMachine2.OwnerReferences = []metav1.OwnerReference{cpOwner}

		machines := []client.Object{
			cpMachine,
			cpMachine2,
			spreadMachine("workers-az3-old", "az3", 45*time.Minute, now),
			spreadMachine("workers-az3-new", "az3", 4*time.Minute, now),
		}

		r, clusterScope := newSpreadReconciler(t, zonalAZs(), machines...)
		require.NoError(t, r.reconcileZonalSpread(context.Background(), clusterScope))

		requireVictims(t, r.Client, "workers-az3-new")
	})

	t.Run("sacrifices the unhealthy extra machine of a zone", func(t *testing.T) {
		// az3 holds two machines, one node-unhealthy: the skeleton is the
		// healthy machine and the unhealthy machine becomes the sacrifice.
		unhealthy := spreadMachine("workers-az3-unhealthy", "az3", 45*time.Minute, now)
		unhealthy.Status.Conditions = []metav1.Condition{{
			Type:               clusterv1.MachineNodeHealthyCondition,
			Status:             metav1.ConditionFalse,
			LastTransitionTime: metav1.NewTime(now.Add(-time.Minute)),
			Reason:             "NodeNotReady",
		}}
		healthy := spreadMachine("workers-az3-healthy", "az3", 4*time.Minute, now)
		az1Worker := spreadMachine("workers-az1", "az1", 20*time.Minute, now)
		az2Worker := spreadMachine("workers-az2", "az2", 15*time.Minute, now)

		r, clusterScope := newSpreadReconciler(t, zonalAZs(), unhealthy, healthy, az1Worker, az2Worker)
		require.NoError(t, r.reconcileZonalSpread(context.Background(), clusterScope))

		requireVictims(t, r.Client, "workers-az3-unhealthy")
	})

	t.Run("resolves the zone from the ProxmoxMachine status on v1.13+ cores", func(t *testing.T) {
		// Reproduces the live rc.13 cluster: worker machines carry no
		// spec.failureDomain (deprecated contract since CAPI v1.13), the
		// zone lives on ProxmoxMachine.status.failureDomain.
		// Spread: az1=1, az2=1, az3=2 -> the az3 extra is the sacrifice.
		az1Worker := spreadMachine("workers-rh5fb", "az1", 74*time.Minute, now)
		az2Worker := spreadMachine("workers-lkgs2", "az2", 54*time.Minute, now)
		az3Old := spreadMachine("workers-kjt5h", "az3", 74*time.Minute, now)
		az3New := spreadMachine("workers-4nml5", "az3", 54*time.Minute, now)
		for _, m := range []*clusterv1.Machine{az1Worker, az2Worker, az3Old, az3New} {
			m.Spec.FailureDomain = "" // as served by CAPI v1.14.4
		}

		machines := make([]client.Object, 0, 8)
		machines = append(machines, az1Worker, az2Worker, az3Old, az3New)
		for name, zone := range map[string]string{
			"workers-rh5fb": "az1",
			"workers-lkgs2": "az2",
			"workers-kjt5h": "az3",
			"workers-4nml5": "az3",
		} {
			machines = append(machines, spreadProxmoxMachine(name, zone))
		}

		r, clusterScope := newSpreadReconciler(t, zonalAZs(), machines...)
		require.NoError(t, r.reconcileZonalSpread(context.Background(), clusterScope))

		_, ok := machineAnnotation(t, r.Client, "workers-4nml5")
		require.True(t, ok, "the az3 extra machine is the sacrifice")
		for _, name := range []string{"workers-rh5fb", "workers-lkgs2", "workers-kjt5h"} {
			_, ok := machineAnnotation(t, r.Client, name)
			require.False(t, ok, "machine %s must not be annotated", name)
		}
	})

	t.Run("keeps annotating machines whose zone is not yet joined", func(t *testing.T) {
		// Zone az3 holds two machines, one without a node ref yet (still
		// provisioning): the skeleton is the joined machine, the
		// provisioning machine is the extra and stays sacrificial.
		provisioning := spreadMachine("workers-az3-provisioning", "az3", 45*time.Minute, now)
		provisioning.Status.NodeRef = clusterv1.MachineNodeReference{}
		healthy := spreadMachine("workers-az3-healthy", "az3", 10*time.Minute, now)
		az1Worker := spreadMachine("workers-az1", "az1", 20*time.Minute, now)
		az2Worker := spreadMachine("workers-az2", "az2", 15*time.Minute, now)

		r, clusterScope := newSpreadReconciler(t, zonalAZs(), provisioning, healthy, az1Worker, az2Worker)
		require.NoError(t, r.reconcileZonalSpread(context.Background(), clusterScope))

		requireVictims(t, r.Client, "workers-az3-provisioning")
	})
}

func TestUpdateAffectsSpread(t *testing.T) {
	now := time.Now()

	t.Run("status churn does not enqueue", func(t *testing.T) {
		machine := spreadMachine("workers-az1", "az1", 20*time.Minute, now)
		updated := machine.DeepCopy()
		updated.Status.Conditions = []metav1.Condition{{Type: clusterv1.MachineReadyCondition, Status: metav1.ConditionTrue}}
		require.False(t, updateAffectsSpread(machine, updated))
	})

	t.Run("spread-relevant changes enqueue", func(t *testing.T) {
		machine := spreadMachine("workers-az1", "az1", 20*time.Minute, now)

		moved := machine.DeepCopy()
		moved.Spec.FailureDomain = "az2"
		require.True(t, updateAffectsSpread(machine, moved))

		deleting := machine.DeepCopy()
		deletionTime := metav1.NewTime(now)
		deleting.DeletionTimestamp = &deletionTime
		require.True(t, updateAffectsSpread(machine, deleting))

		provisioned := machine.DeepCopy()
		provisioned.Spec.ProviderID = "proxmox://1"
		require.True(t, updateAffectsSpread(machine, provisioned))

		booting := machine.DeepCopy()
		booting.Status.NodeRef = clusterv1.MachineNodeReference{}
		joined := machine.DeepCopy()
		joined.Spec.ProviderID = "proxmox://1"
		require.True(t, updateAffectsSpread(booting, joined), "the node join is the moment the machine becomes eligible")
	})
}
