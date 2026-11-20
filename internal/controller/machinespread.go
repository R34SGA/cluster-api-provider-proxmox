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
	"sort"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	infrav1 "github.com/ionos-cloud/cluster-api-provider-proxmox/api/v1alpha2"
	"github.com/ionos-cloud/cluster-api-provider-proxmox/pkg/scope"
)

// machineKind mirrors the clusterv1 kind string used in owner references.
const machineKind = "Machine"

// reconcileZonalSpread keeps MachineSet-owned Machines balanced across the
// cluster's availability zones so a MachineSet scale-down removes a Machine
// from an over-represented zone instead of emptying one at random.
//
// CAPI picks the scale-down victim itself (MachineSet spec.deletion.order,
// which defaults to Random) with no failure-domain awareness: given workers
// spread az1=1/az2=1/az3=2, a scale-down can delete the az1 worker and leave
// az1 empty while az3 keeps two machines. The only hook CAPI honors before
// its own ordering, in every deletion order, is the
// cluster.x-k8s.io/delete-machine annotation: it marks a Machine as the
// preferred victim and never triggers a deletion by itself.
//
// This reconciler splits each MachineSet group into a per-zone skeleton (one
// protected machine per zone, the oldest healthy one) and extra machines, and
// keeps the delete-machine annotation on every extra machine. Whatever the
// size of a scale-down, CAPI removes annotated machines first, so machines
// disappear from over-provisioned zones first and no availability zone can
// end up empty while another one still holds two or more machines. Balanced
// machine sets, machines without a failure
// domain are ignored, and machines not owned by a MachineSet (control-plane
// machines) are skipped. Clusters without availabilityZones are untouched,
// so nothing changes for single-zone clusters.
//
// The zone of a Machine is resolved from the owning ProxmoxMachine's
// status.failureDomain, which is the path CAPI v1.13+ propagates for worker
// machines (spec.failureDomain on InfraMachines is the deprecated contract),
// with Machine.Spec.FailureDomain as a fallback for cores and control-plane
// placements that still set the spec field.
//
// Note: the balancer owns the delete-machine annotation inside MachineSet
// groups of zonal clusters; a manually set annotation on one of those
// machines is treated as balancer state and may be moved or removed.
func (r *ProxmoxClusterReconciler) reconcileZonalSpread(ctx context.Context, clusterScope *scope.ClusterScope) error {
	if len(clusterScope.ProxmoxCluster.Spec.AvailabilityZones) == 0 {
		return nil
	}

	machines := &clusterv1.MachineList{}
	if err := r.List(ctx, machines, client.InNamespace(clusterScope.ProxmoxCluster.GetNamespace())); err != nil {
		return err
	}

	clusterMachines := make([]*clusterv1.Machine, 0, len(machines.Items))
	for i := range machines.Items {
		if machines.Items[i].Spec.ClusterName == clusterScope.ProxmoxCluster.GetName() {
			clusterMachines = append(clusterMachines, &machines.Items[i])
		}
	}

	// The authoritative zone lives on the ProxmoxMachine: CAPI v1.13+ copies
	// it from status.failureDomain onto the Machine's status, and both
	// resources share the same name per the infrastructure contract.
	proxmoxMachines := &infrav1.ProxmoxMachineList{}
	if err := r.List(ctx, proxmoxMachines, client.InNamespace(clusterScope.ProxmoxCluster.GetNamespace())); err != nil {
		return err
	}
	zones := make(map[string]string, len(proxmoxMachines.Items))
	for i := range proxmoxMachines.Items {
		if zone := proxmoxMachines.Items[i].Status.FailureDomain; zone != "" {
			zones[proxmoxMachines.Items[i].GetName()] = zone
		}
	}

	for _, machineset := range groupByMachineSet(clusterMachines) {
		if err := r.balanceMachineSet(ctx, clusterScope, machineset, zones); err != nil {
			return err
		}
	}
	return nil
}

// groupByMachineSet groups machines by the MachineSet that controls them.
// Machines without a MachineSet controller ownerRef (control-plane machines,
// standalone machines) are not part of any group and are skipped.
func groupByMachineSet(machines []*clusterv1.Machine) [][]*clusterv1.Machine {
	bySet := make(map[string][]*clusterv1.Machine)
	names := make([]string, 0)
	for _, machine := range machines {
		name := machineSetOwnerName(machine)
		if name == "" {
			continue
		}
		if _, ok := bySet[name]; !ok {
			names = append(names, name)
		}
		bySet[name] = append(bySet[name], machine)
	}
	sort.Strings(names)

	groups := make([][]*clusterv1.Machine, 0, len(names))
	for _, name := range names {
		groups = append(groups, bySet[name])
	}
	return groups
}

func machineSetOwnerName(machine *clusterv1.Machine) string {
	ref := metav1.GetControllerOf(machine)
	if ref == nil || ref.Kind != "MachineSet" {
		return ""
	}
	return ref.Name
}

// machineZone resolves the availability zone of a Machine: the owning
// ProxmoxMachine's status.failureDomain first, then Machine.Spec.FailureDomain
// (control-plane spread and older CAPI cores still set the spec field).
func machineZone(machine *clusterv1.Machine, zones map[string]string) string {
	if zone, ok := zones[machine.GetName()]; ok {
		return zone
	}
	return machine.Spec.FailureDomain
}

// balanceMachineSet designates the scale-down victims of one MachineSet by
// keeping the delete-machine annotation in sync with the zone balance.
func (r *ProxmoxClusterReconciler) balanceMachineSet(ctx context.Context, clusterScope *scope.ClusterScope, machines []*clusterv1.Machine, zones map[string]string) error {
	victims := spreadVictims(machines, zones)

	for _, machine := range machines {
		// A machine already being deleted keeps its annotations until it goes
		// away; CAPI prioritizes deleting machines first anyway.
		if !machine.GetDeletionTimestamp().IsZero() {
			continue
		}

		_, annotated := machine.GetAnnotations()[clusterv1.DeleteMachineAnnotation]
		_, want := victims[machine.GetName()]
		if annotated == want {
			continue
		}

		before := machine.DeepCopy()
		if want {
			if machine.Annotations == nil {
				machine.Annotations = make(map[string]string)
			}
			machine.Annotations[clusterv1.DeleteMachineAnnotation] = "yes"
		} else {
			delete(machine.Annotations, clusterv1.DeleteMachineAnnotation)
		}

		if err := r.Patch(ctx, machine, client.MergeFrom(before)); err != nil {
			return err
		}

		clusterScope.Logger.V(4).Info("Scale-down designation updated",
			"machine", machine.GetName(), "victim", want,
			"failureDomain", machineZone(machine, zones))
	}
	return nil
}

// spreadVictims returns the machines that must carry the delete-machine
// annotation: every machine beyond its zone's skeleton, where the skeleton is
// the one machine per zone the balancer keeps unannotated (the oldest healthy
// machine, falling back to the oldest machine of the zone when none is
// healthy yet). A MachineSet scale-down removes annotated machines first, so
// however many replicas the user removes at once, machines disappear from the
// over-provisioned zones first and no zone can end up empty while another
// one still holds two or more machines (a single-victim scheme cannot give
// that guarantee: CAPI picks all the victims of a bulk scale-down in one
// pass, and only one of them would have carried the annotation).
//
// The annotation alone never deletes anything: on a balanced set with one
// machine per zone nobody is annotated, and machines currently deleting are
// left outside the computation entirely.
func spreadVictims(machines []*clusterv1.Machine, zones map[string]string) map[string]struct{} {
	byZone := make(map[string][]*clusterv1.Machine)
	for _, machine := range machines {
		if !machine.GetDeletionTimestamp().IsZero() {
			continue
		}
		if zone := machineZone(machine, zones); zone != "" {
			byZone[zone] = append(byZone[zone], machine)
		}
	}

	victims := make(map[string]struct{})
	for _, zoneMachines := range byZone {
		skeleton := zoneMachines[0]
		for _, machine := range zoneMachines[1:] {
			if betterSkeleton(machine, skeleton) {
				skeleton = machine
			}
		}
		for _, machine := range zoneMachines {
			if machine != skeleton {
				victims[machine.GetName()] = struct{}{}
			}
		}
	}
	return victims
}

// betterSkeleton reports whether machine a protects the zone better as the
// skeleton than machine b: a healthy machine over an unhealthy one, then the
// older machine for stability.
func betterSkeleton(a, b *clusterv1.Machine) bool {
	aEligible, bEligible := eligibleForSpread(a), eligibleForSpread(b)
	if aEligible != bEligible {
		return aEligible
	}
	if !a.CreationTimestamp.Time.Equal(b.CreationTimestamp.Time) {
		return a.CreationTimestamp.Time.Before(b.CreationTimestamp.Time)
	}
	return a.GetName() < b.GetName()
}

// eligibleForSpread reports whether the machine can be counted for zone
// balance and can be designated as the scale-down victim. The health part
// mirrors CAPI's own isMachineHealthy (machineset deletion order): machines
// CAPI already prioritizes as unhealthy never carry our annotation. Zone
// resolution failure is handled by the caller, not here.
func eligibleForSpread(machine *clusterv1.Machine) bool {
	if !machine.GetDeletionTimestamp().IsZero() {
		return false
	}
	if !machine.Status.NodeRef.IsDefined() {
		return false
	}
	if conditions.IsFalse(machine, clusterv1.MachineNodeHealthyCondition) {
		return false
	}
	if cond := conditions.Get(machine, clusterv1.MachineHealthCheckSucceededCondition); cond != nil && cond.Status == metav1.ConditionFalse {
		return false
	}
	return true
}

// machineToProxmoxClusterMap enqueues the ProxmoxCluster owning a Machine, so
// the spread is recomputed whenever machines join, leave or move between
// zones. The ProxmoxCluster carries the same name as its Cluster.
func machineToProxmoxClusterMap(_ context.Context, obj client.Object) []reconcile.Request {
	machine, ok := obj.(*clusterv1.Machine)
	if !ok || machine.Spec.ClusterName == "" {
		return nil
	}
	return []reconcile.Request{{
		NamespacedName: client.ObjectKey{Namespace: machine.GetNamespace(), Name: machine.Spec.ClusterName},
	}}
}

// machineSpreadPredicates filters Machine events down to the changes that can
// affect the balance: machines joining or leaving (create/delete), a machine
// entering or leaving deletion, its failure domain changing, the
// delete-machine annotation being flipped, or the providerID appearing (the
// phase where CAPI copies the InfraMachine's zone onto the Machine).
// Plain status churn is ignored.
func machineSpreadPredicates() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc:  func(_ event.CreateEvent) bool { return true },
		DeleteFunc:  func(_ event.DeleteEvent) bool { return true },
		UpdateFunc:  func(e event.UpdateEvent) bool { return updateAffectsSpread(e.ObjectOld, e.ObjectNew) },
		GenericFunc: func(_ event.GenericEvent) bool { return false },
	}
}

func updateAffectsSpread(oldObj, newObj client.Object) bool {
	oldMachine, oldOk := oldObj.(*clusterv1.Machine)
	newMachine, newOk := newObj.(*clusterv1.Machine)
	if !oldOk || !newOk {
		return false
	}
	if (oldMachine.GetDeletionTimestamp() == nil) != (newMachine.GetDeletionTimestamp() == nil) {
		return true
	}
	if oldMachine.Spec.FailureDomain != newMachine.Spec.FailureDomain {
		return true
	}
	if oldMachine.Spec.ProviderID != newMachine.Spec.ProviderID {
		return true
	}
	// The node reference appears when the machine's node joins, the moment
	// the machine becomes eligible for the zone counts: without this trigger
	// the designations would stay stale until the next scale event.
	if oldMachine.Status.NodeRef.Name != newMachine.Status.NodeRef.Name {
		return true
	}
	_, oldAnnotated := oldMachine.GetAnnotations()[clusterv1.DeleteMachineAnnotation]
	_, newAnnotated := newMachine.GetAnnotations()[clusterv1.DeleteMachineAnnotation]
	return oldAnnotated != newAnnotated
}

// proxmoxMachineSpreadPredicates enqueues on ProxmoxMachine events where the
// recorded zone changes: the failure domain is written on the ProxmoxMachine
// when the VM is placed, before CAPI copies it onto the Machine.
func proxmoxMachineSpreadPredicates() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc: func(_ event.CreateEvent) bool { return true },
		DeleteFunc: func(_ event.DeleteEvent) bool { return true },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldMachine, oldOk := e.ObjectOld.(*infrav1.ProxmoxMachine)
			newMachine, newOk := e.ObjectNew.(*infrav1.ProxmoxMachine)
			if !oldOk || !newOk {
				return false
			}
			return oldMachine.Status.FailureDomain != newMachine.Status.FailureDomain
		},
		GenericFunc: func(_ event.GenericEvent) bool { return false },
	}
}

// proxmoxMachineToProxmoxClusterMap enqueues the ProxmoxCluster of the CAPI
// Cluster owning a ProxmoxMachine, resolved through its owner Machine.
func proxmoxMachineToProxmoxClusterMap(c client.Client) handler.MapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		proxmoxMachine, ok := obj.(*infrav1.ProxmoxMachine)
		if !ok {
			return nil
		}
		ref := metav1.GetControllerOf(proxmoxMachine)
		if ref == nil || ref.Kind != machineKind {
			return nil
		}
		machine := &clusterv1.Machine{}
		if err := c.Get(ctx, client.ObjectKey{Namespace: proxmoxMachine.GetNamespace(), Name: ref.Name}, machine); err != nil {
			return nil
		}
		if machine.Spec.ClusterName == "" {
			return nil
		}
		return []reconcile.Request{{
			NamespacedName: client.ObjectKey{Namespace: machine.GetNamespace(), Name: machine.Spec.ClusterName},
		}}
	}
}
