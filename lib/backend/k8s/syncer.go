// Copyright (c) 2016 Tigera, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package k8s

import (
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/projectcalico/libcalico-go/lib/backend/api"
	"github.com/projectcalico/libcalico-go/lib/backend/compat"
	"github.com/projectcalico/libcalico-go/lib/backend/model"
	k8sapi "k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/apis/extensions"
	"k8s.io/kubernetes/pkg/watch"
	"reflect"
)

func newSyncer(kc KubeClient, callbacks api.SyncerCallbacks) *kubeSyncer {
	return &kubeSyncer{
		kc:        kc,
		callbacks: callbacks,
	}
}

type kubeSyncer struct {
	kc        KubeClient
	callbacks api.SyncerCallbacks
	OneShot   bool
}

// Holds resource version information.
type resourceVersions struct {
	podVersion           string
	namespaceVersion     string
	networkPolicyVersion string
}

func getUpdateType(e watch.Event) api.UpdateType {
	// Determine the update type.
	updateType := api.UpdateTypeKVUnknown
	switch e.Type {
	case watch.Added:
		updateType = api.UpdateTypeKVNew
	case watch.Deleted:
		updateType = api.UpdateTypeKVDeleted
	case watch.Modified:
		updateType = api.UpdateTypeKVUpdated
	}
	return updateType
}

func (syn *kubeSyncer) Start() {
	// Start a background thread to read snapshots from and watch the Kubernetes API,
	// and pass updates via callbacks.
	go syn.readFromKubernetesAPI()
}

func (syn *kubeSyncer) readFromKubernetesAPI() {
	log.Info("Starting Kubernetes API read worker")

	// Keep track of the latest resource versions.
	latestVersions := resourceVersions{}

	// Other watcher vars.
	var nsChan, poChan, npChan <-chan watch.Event
	var event watch.Event
	var upd *api.Update
	var opts k8sapi.ListOptions

	// Always perform an initial snapshot.
	needsResync := true

	log.Info("Starting Kubernetes API read loop")
	for {
		// If we need to resync, do so.
		if needsResync {
			// Set status to ResyncInProgress.
			log.Warnf("Resync required - latest versions: %+v", latestVersions)
			syn.callbacks.OnStatusUpdated(api.ResyncInProgress)

			// Perform the snapshot and update the status.
			syn.performSnapshot(&latestVersions)
			log.Warnf("Snapshot complete - start watch from %+v", latestVersions)
			syn.callbacks.OnStatusUpdated(api.InSync)

			// Create the Kubernetes API watchers.
			opts = k8sapi.ListOptions{ResourceVersion: latestVersions.namespaceVersion}
			nsWatch, err := syn.kc.clientSet.Namespaces().Watch(opts)
			if err != nil {
				log.Warn("Failed to connect to API, retrying")
				time.Sleep(1 * time.Second)
				continue
			}
			opts = k8sapi.ListOptions{ResourceVersion: latestVersions.podVersion}
			poWatch, err := syn.kc.clientSet.Pods("").Watch(opts)
			if err != nil {
				log.Warn("Failed to connect to API, retrying")
				time.Sleep(1 * time.Second)
				continue
			}
			opts = k8sapi.ListOptions{ResourceVersion: latestVersions.networkPolicyVersion}
			npWatch, err := syn.kc.clientSet.NetworkPolicies("").Watch(opts)
			if err != nil {
				log.Warn("Failed to connect to API, retrying")
				time.Sleep(1 * time.Second)
				continue
			}

			nsChan = nsWatch.ResultChan()
			poChan = poWatch.ResultChan()
			npChan = npWatch.ResultChan()

			// Success - reset the flag.
			needsResync = false
		}

		// Don't start watches if we're in oneshot mode.
		if syn.OneShot {
			return
		}

		// Select on the various watch channels.
		select {
		case event = <-nsChan:
			log.Debugf("Incoming Namespace watch event. Type=%s", event.Type)
			if needsResync = syn.eventTriggersResync(event); needsResync {
				// We need to resync.  Break out of the back into the sync loop.
				log.Warn("Event triggered resync: %+v", event)
				continue
			}

			// Event is OK - parse it.
			upds := syn.parseNamespaceEvent(event)
			latestVersions.namespaceVersion = upds[0].KVPair.Revision.(string)
			syn.callbacks.OnUpdates(upds)
			continue
		case event = <-poChan:
			log.Debugf("Incoming Pod watch event. Type=%s", event.Type)
			if needsResync = syn.eventTriggersResync(event); needsResync {
				// We need to resync.  Break out of the back into the sync loop.
				log.Warn("Event triggered resync: %+v", event)
				continue
			}

			// Event is OK - parse it.
			if upd = syn.parsePodEvent(event); upd != nil {
				// Only send the update if we care about it.  We filter
				// out a number of events that aren't useful for us.
				latestVersions.podVersion = upd.KVPair.Revision.(string)
				syn.callbacks.OnUpdates([]api.Update{*upd})
			}
		case event = <-npChan:
			log.Debugf("Incoming NetworkPolicy watch event. Type=%s", event.Type)
			if needsResync = syn.eventTriggersResync(event); needsResync {
				// We need to resync.  Break out of the back into the sync loop.
				log.Warn("Event triggered resync: %+v", event)
				continue
			}

			// Event is OK - parse it and send it over the channel.
			upd = syn.parseNetworkPolicyEvent(event)
			latestVersions.networkPolicyVersion = upd.KVPair.Revision.(string)
			syn.callbacks.OnUpdates([]api.Update{*upd})
		}
	}
}

// performSnapshot returns a list of existing objects in the datastore, and
// populates the provided resourceVersions with the latest k8s resource version
// for each.
func (syn *kubeSyncer) performSnapshot(versions *resourceVersions) *[]api.Update {
	snap := []api.Update{}
	opts := k8sapi.ListOptions{}

	// Loop until we successfully are able to accesss the API.
	for {
		// Get Namespaces (Profiles)
		log.Info("Syncing Namespaces")
		nsList, err := syn.kc.clientSet.Namespaces().List(opts)
		if err != nil {
			log.Warnf("Error accessing Kubernetes API, retrying: %s", err)
			time.Sleep(1 * time.Second)
			continue
		}
		versions.namespaceVersion = nsList.ListMeta.ResourceVersion
		for _, ns := range nsList.Items {
			// The Syncer API expects a profile to be broken into its underlying
			// components - rules, tags, labels.
			profile, err := syn.kc.converter.namespaceToProfile(&ns)
			if err != nil {
				log.Panicf("%s", err)
			}
			rules, tags, labels := compat.ToTagsLabelsRules(profile)
			rules.Revision = profile.Revision
			tags.Revision = profile.Revision
			labels.Revision = profile.Revision

			snap = append(snap,
				api.Update{KVPair: *rules, UpdateType: api.UpdateTypeKVNew},
				api.Update{KVPair: *tags, UpdateType: api.UpdateTypeKVNew},
				api.Update{KVPair: *labels, UpdateType: api.UpdateTypeKVNew},
			)

			// If this is the kube-system Namespace, also send
			// the pool through. // TODO: Hacky.
			if ns.ObjectMeta.Name == "kube-system" {
				pool, _ := syn.kc.converter.namespaceToPool(&ns)
				if pool != nil {
					snap = append(snap, api.Update{KVPair: *pool, UpdateType: api.UpdateTypeKVNew})
				}
			}
		}

		// Get NetworkPolicies (Policies)
		log.Info("Syncing NetworkPolicy")
		npList, err := syn.kc.clientSet.NetworkPolicies("").List(opts)
		if err != nil {
			log.Warnf("Error accessing Kubernetes API, retrying: %s", err)
			time.Sleep(1 * time.Second)
			continue
		}

		versions.networkPolicyVersion = npList.ListMeta.ResourceVersion
		for _, np := range npList.Items {
			pol, _ := syn.kc.converter.networkPolicyToPolicy(&np)
			snap = append(snap, api.Update{KVPair: *pol, UpdateType: api.UpdateTypeKVNew})
		}

		// Get Pods (WorkloadEndpoints)
		log.Info("Syncing Pods")
		poList, err := syn.kc.clientSet.Pods("").List(opts)
		if err != nil {
			log.Warnf("Error accessing Kubernetes API, retrying: %s", err)
			time.Sleep(1 * time.Second)
			continue
		}

		versions.podVersion = poList.ListMeta.ResourceVersion
		for _, po := range poList.Items {
			wep, _ := syn.kc.converter.podToWorkloadEndpoint(&po)
			if wep != nil {
				snap = append(snap, api.Update{KVPair: *wep, UpdateType: api.UpdateTypeKVNew})
			}
		}

		// Sync GlobalConfig.
		confList, err := syn.kc.listGlobalConfig(model.GlobalConfigListOptions{})
		if err != nil {
			log.Warnf("Error accessing Kubernetes API, retrying: %s", err)
			time.Sleep(1 * time.Second)
			continue
		}

		for _, c := range confList {
			snap = append(snap, api.Update{KVPair: *c, UpdateType: api.UpdateTypeKVNew})
		}

		// Include ready state.
		ready, err := syn.kc.getReadyStatus(model.ReadyFlagKey{})
		if err != nil {
			log.Warnf("Error accessing Kubernetes API, retrying: %s", err)
			time.Sleep(1 * time.Second)
			continue
		}
		snap = append(snap, api.Update{KVPair: *ready, UpdateType: api.UpdateTypeKVNew})

		log.Infof("Snapshot resourceVersions: %+v", versions)
		log.Debugf("Created snapshot: %+v", snap)
		return &snap
	}
}

// watchKubeAPI watches the Kubernetes API and sends updates to the merge thread.
// If it encounters an error or falls behind, it triggers a new snapshot.
func (syn *kubeSyncer) watchKubeAPI(updateChan chan *api.Update,
	resyncChan chan *resourceVersions, initialVersionSource chan *resourceVersions) {
}

// eventTriggersResync returns true of the given event requires a
// full datastore resync to occur, and false otherwise.
func (syn *kubeSyncer) eventTriggersResync(e watch.Event) bool {
	// If we encounter an error, or if the event is nil (which can indicate
	// an unexpected connection close).
	if e.Type == watch.Error || e.Object == nil {
		return true
	}
	return false
}

func (syn *kubeSyncer) parseNamespaceEvent(e watch.Event) []api.Update {
	ns, ok := e.Object.(*k8sapi.Namespace)
	if !ok {
		log.Panicf("Invalid namespace event: %+v", e.Object)
	}

	// Convert the received Namespace into a profile KVPair.
	profile, err := syn.kc.converter.namespaceToProfile(ns)
	if err != nil {
		log.Panicf("%s", err)
	}
	rules, tags, labels := compat.ToTagsLabelsRules(profile)
	rules.Revision = profile.Revision
	tags.Revision = profile.Revision
	labels.Revision = profile.Revision

	// If this is the kube-system Namespace, it also houses Pool
	// information, so send a pool update. FIXME: Make this better.
	var pool *model.KVPair
	if ns.ObjectMeta.Name == "kube-system" {
		pool, err = syn.kc.converter.namespaceToPool(ns)
		if err != nil {
			log.Panicf("%s", err)
		}
	}

	// For deletes, we need to nil out the Value part of the KVPair.
	if e.Type == watch.Deleted {
		rules.Value = nil
		tags.Value = nil
		labels.Value = nil
	}

	// Return the updates.
	updateType := getUpdateType(e)
	updates := []api.Update{
		api.Update{KVPair: *rules, UpdateType: updateType},
		api.Update{KVPair: *tags, UpdateType: updateType},
		api.Update{KVPair: *labels, UpdateType: updateType},
	}
	if pool != nil {
		updates = append(updates, api.Update{KVPair: *pool, UpdateType: updateType})
	}
	return updates
}

// labelCache stores labels for a given pod, so we can determine if
// we need to send an update on the syncer API for a given event.
var labelCache map[string]map[string]string = map[string]map[string]string{}

// parsePodEvent returns a KVPair for the given event.  If the event isn't
// useful, parsePodEvent returns nil to indicate that there is nothing to do.
func (syn *kubeSyncer) parsePodEvent(e watch.Event) *api.Update {
	pod, ok := e.Object.(*k8sapi.Pod)
	if !ok {
		log.Panicf("Invalid pod event. Type: %s, Object: %+v", e.Type, e.Object)
	}

	// Ignore any updates for host networked pods.
	if syn.kc.converter.isHostNetworked(pod) {
		log.Debugf("Skipping host networked pod %s/%s", pod.ObjectMeta.Namespace, pod.ObjectMeta.Name)
		return nil
	}

	// Convert the received Namespace into a KVPair.
	kvp, err := syn.kc.converter.podToWorkloadEndpoint(pod)
	if err != nil {
		log.Panicf("%s", err)
	}

	// We behave differently based on the event type.
	switch e.Type {
	case watch.Deleted:
		// For deletes, we need to nil out the Value part of the KVPair.
		log.Debugf("Delete for pod %s/%s", pod.ObjectMeta.Namespace, pod.ObjectMeta.Name)
		kvp.Value = nil

		// Remove it from the cache, if it is there.
		workload := kvp.Key.(model.WorkloadEndpointKey).WorkloadID
		delete(labelCache, workload)
	default:
		// Adds and modifies are treated the same.  First, if the pod doesn't have an
		// IP address, we ignore it until it does.
		if !syn.kc.converter.hasIPAddress(pod) {
			log.Debugf("Skipping pod with no IP: %s/%s", pod.ObjectMeta.Namespace, pod.ObjectMeta.Name)
			return nil
		}

		// If it does have an address, we only send updates if the labels have changed.
		workload := kvp.Key.(model.WorkloadEndpointKey).WorkloadID
		labels := kvp.Value.(*model.WorkloadEndpoint).Labels
		if reflect.DeepEqual(labelCache[workload], labels) {
			// Labels haven't changed - no need to send an update for this add/modify.
			log.Debugf("Skipping pod event - labels didn't change: %s/%s", pod.ObjectMeta.Namespace, pod.ObjectMeta.Name)
			return nil
		}

		// Labels have changed on a running pod - update the label cache.
		labelCache[workload] = labels
	}

	return &api.Update{KVPair: *kvp, UpdateType: getUpdateType(e)}
}

func (syn *kubeSyncer) parseNetworkPolicyEvent(e watch.Event) *api.Update {
	log.Debug("Parsing NetworkPolicy watch event")
	// First, check the event type.
	np, ok := e.Object.(*extensions.NetworkPolicy)
	if !ok {
		log.Panicf("Invalid NetworkPolicy event. Type: %s, Object: %+v", e.Type, e.Object)
	}

	// Convert the received NetworkPolicy into a profile KVPair.
	kvp, err := syn.kc.converter.networkPolicyToPolicy(np)
	if err != nil {
		log.Panicf("%s", err)
	}

	// For deletes, we need to nil out the Value part of the KVPair.
	if e.Type == watch.Deleted {
		kvp.Value = nil
	}
	return &api.Update{KVPair: *kvp, UpdateType: getUpdateType(e)}
}
