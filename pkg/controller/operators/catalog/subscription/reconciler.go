package subscription

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"

	"github.com/operator-framework/api/pkg/operators/reference"
	"github.com/operator-framework/api/pkg/operators/v1alpha1"
	"github.com/operator-framework/operator-lifecycle-manager/pkg/api/client/clientset/versioned"
	listers "github.com/operator-framework/operator-lifecycle-manager/pkg/api/client/listers/operators/v1alpha1"
	"github.com/operator-framework/operator-lifecycle-manager/pkg/controller/registry/reconciler"
	"github.com/operator-framework/operator-lifecycle-manager/pkg/controller/registry/resolver/cache"
	"github.com/operator-framework/operator-lifecycle-manager/pkg/lib/kubestate"
	"github.com/operator-framework/operator-lifecycle-manager/pkg/lib/queueinformer"
	"github.com/operator-framework/operator-registry/pkg/api"
)

// ReconcilerFromLegacySyncHandler returns a reconciler that invokes the given legacy sync handler and on delete funcs.
// Since the reconciler does not return an updated kubestate, it MUST be the last reconciler in a given chain.
func ReconcilerFromLegacySyncHandler(sync queueinformer.LegacySyncHandler) kubestate.Reconciler {
	var rec kubestate.ReconcilerFunc = func(ctx context.Context, in kubestate.State) (out kubestate.State, err error) {
		out = in
		switch s := in.(type) {
		case SubscriptionExistsState:
			if sync != nil {
				err = sync(s.Subscription())
			}
		case SubscriptionState:
			if sync != nil {
				err = sync(s.Subscription())
			}
		default:
			utilruntime.HandleError(fmt.Errorf("unexpected subscription state in legacy reconciler: %T", s))
		}

		return
	}

	return rec
}

// catalogHealthReconciler reconciles catalog health status for subscriptions.
type catalogHealthReconciler struct {
	now                       func() *metav1.Time
	client                    versioned.Interface
	catalogLister             listers.CatalogSourceLister
	csvLister                 listers.ClusterServiceVersionLister
	registryReconcilerFactory reconciler.RegistryReconcilerFactory
	globalCatalogNamespace    string
	operatorCacheProvider     cache.OperatorCacheProvider
	logger                    logrus.StdLogger
}

// Reconcile reconciles subscription catalog health conditions.
func (c *catalogHealthReconciler) Reconcile(ctx context.Context, in kubestate.State) (out kubestate.State, err error) {
	next := in
	var prev kubestate.State

	// loop until this state can no longer transition
	for err == nil && next != nil && next != prev && !next.Terminal() {
		select {
		case <-ctx.Done():
			err = errors.New("subscription catalog health reconciliation context closed")
		default:
			prev = next

			switch s := next.(type) {
			case CatalogHealthKnownState:
				// Target state already known, no work to do
				next = s
			case CatalogHealthState:
				// Gather catalog health and transition state
				ns := s.Subscription().GetNamespace()
				var catalogHealth []v1alpha1.SubscriptionCatalogHealth
				if catalogHealth, err = c.catalogHealth(ns); err != nil {
					break
				}

				var healthUpdated bool
				next, healthUpdated = s.UpdateHealth(c.now(), catalogHealth...)

				deprecationUpdated, err := c.updateDeprecatedStatus(ctx, s.Subscription())
				if err != nil {
					return next, err
				}
				lifecycleUpdated, err := c.updateLifecycleStatus(ctx, s.Subscription())
				if err != nil {
					return next, err
				}
				if healthUpdated || deprecationUpdated || lifecycleUpdated {
					if _, err := c.client.OperatorsV1alpha1().Subscriptions(ns).UpdateStatus(ctx, s.Subscription(), metav1.UpdateOptions{}); err != nil {
						return next, err
					}
				}
			case SubscriptionExistsState:
				if s == nil {
					err = errors.New("nil state")
					break
				}
				if s.Subscription() == nil {
					err = errors.New("nil subscription in state")
					break
				}

				// Set up fresh state
				next = NewCatalogHealthState(s)
			default:
				// Ignore all other typestates
				next = s
			}
		}
	}

	out = next

	return
}

// updateDeprecatedStatus adds deprecation status conditions to the subscription when present in the cache entry then
// returns a bool value of true if any changes to the existing subscription have occurred.
func (c *catalogHealthReconciler) updateDeprecatedStatus(ctx context.Context, sub *v1alpha1.Subscription) (bool, error) {
	if c.operatorCacheProvider == nil {
		return false, nil
	}

	entries := c.operatorCacheProvider.Namespaced(sub.Spec.CatalogSourceNamespace).Catalog(cache.SourceKey{
		Name:      sub.Spec.CatalogSource,
		Namespace: sub.Spec.CatalogSourceNamespace,
	}).Find(cache.PkgPredicate(sub.Spec.Package), cache.ChannelPredicate(sub.Spec.Channel))

	if len(entries) == 0 {
		return false, nil
	}

	changed := false
	rollupMessages := []string{}
	var deprecations *cache.Deprecations

	found := false
	for _, entry := range entries {
		// Find the cache entry that matches this subscription
		if entry.SourceInfo == nil {
			continue
		}
		if sub.Status.InstalledCSV != entry.Name {
			continue
		}
		deprecations = entry.SourceInfo.Deprecations
		found = true
		break
	}
	if !found {
		// No matching entry found
		return false, nil
	}
	conditionTypes := []v1alpha1.SubscriptionConditionType{
		v1alpha1.SubscriptionPackageDeprecated,
		v1alpha1.SubscriptionChannelDeprecated,
		v1alpha1.SubscriptionBundleDeprecated,
	}
	for _, conditionType := range conditionTypes {
		oldCondition := sub.Status.GetCondition(conditionType)
		var deprecation *api.Deprecation
		if deprecations != nil {
			switch conditionType {
			case v1alpha1.SubscriptionPackageDeprecated:
				deprecation = deprecations.Package
			case v1alpha1.SubscriptionChannelDeprecated:
				deprecation = deprecations.Channel
			case v1alpha1.SubscriptionBundleDeprecated:
				deprecation = deprecations.Bundle
			}
		}
		if deprecation != nil {
			if conditionType == v1alpha1.SubscriptionChannelDeprecated && sub.Spec.Channel == "" {
				// Special case: If optional field sub.Spec.Channel is unset do not apply a channel
				// deprecation message and remove them if any exist.
				sub.Status.RemoveConditions(conditionType)
				if oldCondition.Status == corev1.ConditionTrue {
					changed = true
				}
				continue
			}
			newCondition := v1alpha1.SubscriptionCondition{
				Type:               conditionType,
				Message:            deprecation.Message,
				Status:             corev1.ConditionTrue,
				LastTransitionTime: c.now(),
			}
			rollupMessages = append(rollupMessages, deprecation.Message)
			if oldCondition.Message != newCondition.Message {
				// oldCondition's message was empty or has changed; add or update the condition
				sub.Status.SetCondition(newCondition)
				changed = true
			}
		} else if oldCondition.Status == corev1.ConditionTrue {
			// No longer deprecated at this level; remove the condition
			sub.Status.RemoveConditions(conditionType)
			changed = true
		}
	}

	if !changed {
		// No need to update rollup condition if no other conditions have changed
		return false, nil
	}
	if len(rollupMessages) > 0 {
		rollupCondition := v1alpha1.SubscriptionCondition{
			Type:               v1alpha1.SubscriptionDeprecated,
			Message:            strings.Join(rollupMessages, "; "),
			Status:             corev1.ConditionTrue,
			LastTransitionTime: c.now(),
		}
		sub.Status.SetCondition(rollupCondition)
	} else {
		// No rollup message means no deprecation conditions were set; remove the rollup if it exists
		sub.Status.RemoveConditions(v1alpha1.SubscriptionDeprecated)
	}

	return true, nil
}

// updateLifecycleStatus updates lifecycle and compatibility status on the subscription
// based on the installed CSV's minor version. Returns true if any changes occurred.
func (c *catalogHealthReconciler) updateLifecycleStatus(_ context.Context, sub *v1alpha1.Subscription) (bool, error) {
	if c.operatorCacheProvider == nil || c.csvLister == nil {
		return false, nil
	}

	// Get the installed CSV name
	if sub.Status.InstalledCSV == "" {
		return false, nil
	}

	// Look up the CSV to get its version
	csv, err := c.csvLister.ClusterServiceVersions(sub.GetNamespace()).Get(sub.Status.InstalledCSV)
	if err != nil {
		// CSV not found - leave status untouched (could be transitional state)
		return false, nil
	}

	// Get minor version string from CSV spec.version (e.g., "1.2")
	// OperatorVersion is a value type wrapping semver.Version, so it's always usable
	minorVersion := fmt.Sprintf("%d.%d", csv.Spec.Version.Version.Major, csv.Spec.Version.Version.Minor)

	// Lookup lifecycle info via PackageInfo from catalog cache
	catalogKey := cache.SourceKey{
		Name:      sub.Spec.CatalogSource,
		Namespace: sub.Spec.CatalogSourceNamespace,
	}
	catalog := c.operatorCacheProvider.Namespaced(sub.Spec.CatalogSourceNamespace).Catalog(catalogKey)

	pkgInfo := catalog.GetPackageInfo(sub.Spec.Package)
	if pkgInfo == nil {
		return c.clearLifecycleStatus(sub)
	}

	lifecycleInfo := pkgInfo.GetVersionLifecycle(minorVersion)
	if lifecycleInfo == nil {
		// No lifecycle info for this minor version - clear status
		return c.clearLifecycleStatus(sub)
	}

	return c.setLifecycleStatus(sub, lifecycleInfo)
}

func (c *catalogHealthReconciler) clearLifecycleStatus(sub *v1alpha1.Subscription) (bool, error) {
	changed := false
	if sub.Status.Lifecycle != nil {
		sub.Status.Lifecycle = nil
		changed = true
	}
	if sub.Status.Compatibility != nil {
		sub.Status.Compatibility = nil
		changed = true
	}
	return changed, nil
}

func (c *catalogHealthReconciler) setLifecycleStatus(sub *v1alpha1.Subscription, info *cache.VersionLifecycleInfo) (bool, error) {
	changed := false

	// Build SubscriptionLifecycleStatus from phases
	var newLifecycle *v1alpha1.SubscriptionLifecycleStatus
	if len(info.Phases) > 0 {
		var err error
		newLifecycle, err = c.buildLifecycleStatus(info.Phases)
		if err != nil {
			return false, fmt.Errorf("failed to build lifecycle status: %w", err)
		}
	}
	if !lifecycleEqual(sub.Status.Lifecycle, newLifecycle) {
		sub.Status.Lifecycle = newLifecycle
		changed = true
	}

	// Build SubscriptionCompatibility from compatibility
	var newCompat *v1alpha1.SubscriptionCompatibility
	if len(info.Compatibility) > 0 {
		newCompat = c.buildCompatibilityStatus(info.Compatibility)
	}
	if !compatibilityEqual(sub.Status.Compatibility, newCompat) {
		sub.Status.Compatibility = newCompat
		changed = true
	}

	return changed, nil
}


func (c *catalogHealthReconciler) buildLifecycleStatus(phases []*api.LifecyclePhase) (*v1alpha1.SubscriptionLifecycleStatus, error) {
	return computeLifecycleStatus(phases, c.now().Time)
}

// computeLifecycleStatus determines the current and next lifecycle phases based on the
// provided time. This function is extracted for testability.
//
// Phase timeline requirements:
//   - Exactly one phase MUST have StartDate unset (empty string), meaning it starts at the
//     beginning of time (time.Time{} zero value). This will be the first phase after sorting.
//   - Exactly one phase MUST have EndDate unset (empty string), meaning it extends to infinity.
//     This will be the last phase after sorting.
//   - Phases must be contiguous and non-overlapping, covering all time from negative infinity
//     to positive infinity.
//   - StartDate is inclusive, EndDate is exclusive: a phase covers [startDate, endDate).
//   - Dates must be in RFC3339 format when present.
//   - Phases are sorted by StartDate automatically; input order does not matter.
//
// Behavior:
//   - Returns (nil, nil) if phases slice is empty or nil.
//   - Returns an error if no phase has StartDate unset (no phase starts at beginning of time).
//   - Returns an error if no phase has EndDate unset (no phase extends to infinity).
//   - Returns an error if phase dates cannot be parsed.
//   - The current phase is the one where: startTime <= now < endTime (or no endTime).
//   - The next phase is the phase immediately following the current phase after sorting.
func computeLifecycleStatus(phases []*api.LifecyclePhase, now time.Time) (*v1alpha1.SubscriptionLifecycleStatus, error) {
	if len(phases) == 0 {
		return nil, nil
	}

	// Parse all phases first to validate dates
	type parsedPhase struct {
		phase     *api.LifecyclePhase
		startTime time.Time
		endTime   time.Time
		hasEnd    bool
	}
	parsed := make([]parsedPhase, 0, len(phases))

	for _, phase := range phases {
		var startTime time.Time
		if phase.GetStartDate() != "" {
			var err error
			startTime, err = time.Parse(time.RFC3339, phase.GetStartDate())
			if err != nil {
				return nil, fmt.Errorf("invalid StartDate %q for phase %q: %w", phase.GetStartDate(), phase.GetName(), err)
			}
		}
		// startTime defaults to zero value if StartDate is empty

		var endTime time.Time
		hasEnd := phase.GetEndDate() != ""
		if hasEnd {
			var err error
			endTime, err = time.Parse(time.RFC3339, phase.GetEndDate())
			if err != nil {
				return nil, fmt.Errorf("invalid EndDate %q for phase %q: %w", phase.GetEndDate(), phase.GetName(), err)
			}
		}

		parsed = append(parsed, parsedPhase{
			phase:     phase,
			startTime: startTime,
			endTime:   endTime,
			hasEnd:    hasEnd,
		})
	}

	// Sort phases by start time (empty StartDate = zero time sorts first)
	sort.Slice(parsed, func(i, j int) bool {
		return parsed[i].startTime.Before(parsed[j].startTime)
	})

	// Validate: first phase (after sorting) must start at beginning of time
	if parsed[0].phase.GetStartDate() != "" {
		return nil, fmt.Errorf("no phase starts at beginning of time: first phase %q has StartDate %q", parsed[0].phase.GetName(), parsed[0].phase.GetStartDate())
	}

	// Validate: last phase (after sorting) must extend to infinity
	if parsed[len(parsed)-1].hasEnd {
		return nil, fmt.Errorf("no phase extends to infinity: last phase %q has EndDate %q", parsed[len(parsed)-1].phase.GetName(), parsed[len(parsed)-1].phase.GetEndDate())
	}

	// Validate: phases must be contiguous (each phase's EndDate == next phase's StartDate)
	for i := 0; i < len(parsed)-1; i++ {
		current := parsed[i]
		next := parsed[i+1]
		if !current.endTime.Equal(next.startTime) {
			return nil, fmt.Errorf("phases are not contiguous: phase %q ends at %s but phase %q starts at %s",
				current.phase.GetName(), current.phase.GetEndDate(),
				next.phase.GetName(), next.phase.GetStartDate())
		}
	}

	// Find current phase: startTime <= now < endTime (or no end)
	var currentIdx int = -1
	for i, p := range parsed {
		inPhase := !now.Before(p.startTime) && (!p.hasEnd || now.Before(p.endTime))
		if inPhase {
			currentIdx = i
			break // Phases are sorted, first match is the current phase
		}
	}

	if currentIdx < 0 {
		// This shouldn't happen if phases are contiguous, but handle it gracefully
		return nil, fmt.Errorf("no phase covers the current time")
	}

	current := parsed[currentIdx]
	status := &v1alpha1.SubscriptionLifecycleStatus{
		CurrentPhase: current.phase.GetName(),
	}

	if current.hasEnd {
		t := metav1.NewTime(current.endTime)
		status.CurrentPhaseEndDate = &t
	}

	// Next phase is the one immediately after current in the slice
	if currentIdx+1 < len(parsed) {
		status.NextPhase = parsed[currentIdx+1].phase.GetName()
	}

	return status, nil
}

func (c *catalogHealthReconciler) buildCompatibilityStatus(compat []*api.PlatformCompatibility) *v1alpha1.SubscriptionCompatibility {
	// Simply copy the compatibility data to the subscription status.
	// Let clients determine whether the current platform is compatible.
	if len(compat) == 0 {
		return nil
	}
	platforms := make([]v1alpha1.PlatformVersions, 0, len(compat))
	for _, pc := range compat {
		platforms = append(platforms, v1alpha1.PlatformVersions{
			Platform: pc.Platform,
			Versions: pc.Versions,
		})
	}
	return &v1alpha1.SubscriptionCompatibility{
		CompatiblePlatforms: platforms,
	}
}

func lifecycleEqual(a, b *v1alpha1.SubscriptionLifecycleStatus) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if a.CurrentPhase != b.CurrentPhase {
		return false
	}
	if a.NextPhase != b.NextPhase {
		return false
	}
	// Compare end dates
	if (a.CurrentPhaseEndDate == nil) != (b.CurrentPhaseEndDate == nil) {
		return false
	}
	if a.CurrentPhaseEndDate != nil && b.CurrentPhaseEndDate != nil {
		if !a.CurrentPhaseEndDate.Equal(b.CurrentPhaseEndDate) {
			return false
		}
	}
	return true
}

func compatibilityEqual(a, b *v1alpha1.SubscriptionCompatibility) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return reflect.DeepEqual(a.CompatiblePlatforms, b.CompatiblePlatforms)
}

// catalogHealth gets the health of catalogs that can affect Susbcriptions in the given namespace.
// This means all catalogs in the given namespace, as well as any catalogs in the operator's global catalog namespace.
func (c *catalogHealthReconciler) catalogHealth(namespace string) ([]v1alpha1.SubscriptionCatalogHealth, error) {
	catalogs, err := c.catalogLister.CatalogSources(namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	if namespace != c.globalCatalogNamespace {
		globals, err := c.catalogLister.CatalogSources(c.globalCatalogNamespace).List(labels.Everything())
		if err != nil {
			return nil, err
		}

		catalogs = append(catalogs, globals...)
	}

	// Sort to ensure ordering
	sort.Slice(catalogs, func(i, j int) bool {
		return catalogs[i].GetNamespace()+catalogs[i].GetName() < catalogs[j].GetNamespace()+catalogs[j].GetName()
	})

	catalogHealth := make([]v1alpha1.SubscriptionCatalogHealth, len(catalogs))
	now := c.now()
	var errs []error
	for i, catalog := range catalogs {
		h, err := c.health(now, catalog)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		// Prevent assignment when any error has been encountered since the results will be discarded
		if errs == nil {
			catalogHealth[i] = *h
		}
	}

	if errs != nil || len(catalogHealth) == 0 {
		// Assign meaningful zero value
		catalogHealth = nil
	}

	return catalogHealth, utilerrors.NewAggregate(errs)
}

// health returns a SusbcriptionCatalogHealth for the given catalog with the given now.
func (c *catalogHealthReconciler) health(now *metav1.Time, catalog *v1alpha1.CatalogSource) (*v1alpha1.SubscriptionCatalogHealth, error) {
	healthy, err := c.healthy(catalog)
	if err != nil {
		return nil, err
	}

	ref, err := reference.GetReference(catalog)
	if err != nil {
		return nil, err
	}
	if ref == nil {
		return nil, errors.New("nil reference")
	}

	h := &v1alpha1.SubscriptionCatalogHealth{
		CatalogSourceRef: ref,
		// TODO: Should LastUpdated be set here, or at time of subscription update?
		LastUpdated: now,
		Healthy:     healthy,
	}

	return h, nil
}

// healthy returns true if the given catalog is healthy, false otherwise, and any error encountered
// while checking the catalog's registry server.
func (c *catalogHealthReconciler) healthy(catalog *v1alpha1.CatalogSource) (bool, error) {
	if catalog.Status.Reason == v1alpha1.CatalogSourceSpecInvalidError {
		// The catalog's spec is bad, mark unhealthy
		return false, nil
	}

	// Check connection health
	rec := c.registryReconcilerFactory.ReconcilerForSource(catalog)
	if rec == nil {
		return false, fmt.Errorf("could not get reconciler for catalog: %#v", catalog)
	}

	return rec.CheckRegistryServer(logrus.NewEntry(logrus.New()), catalog)
}

// installPlanReconciler reconciles InstallPlan status for Subscriptions.
type installPlanReconciler struct {
	now               func() *metav1.Time
	client            versioned.Interface
	installPlanLister listers.InstallPlanLister
}

// Reconcile reconciles Subscription InstallPlan conditions.
func (i *installPlanReconciler) Reconcile(ctx context.Context, in kubestate.State) (out kubestate.State, err error) {
	next := in
	var prev kubestate.State

	// loop until this state can no longer transition
	for err == nil && next != nil && prev != next && !next.Terminal() {
		select {
		case <-ctx.Done():
			err = errors.New("subscription installplan reconciliation context closed")
		default:
			prev = next

			switch s := next.(type) {
			case NoInstallPlanReferencedState:
				// No InstallPlan was referenced, no work to do
				next = s
			case InstallPlanKnownState:
				// Target state already known, no work to do
				next = s
			case InstallPlanReferencedState:
				// Check the stated InstallPlan
				ref := s.Subscription().Status.InstallPlanRef // Should never be nil in this typestate
				subClient := i.client.OperatorsV1alpha1().Subscriptions(ref.Namespace)

				var plan *v1alpha1.InstallPlan
				if plan, err = i.installPlanLister.InstallPlans(ref.Namespace).Get(ref.Name); err != nil {
					if apierrors.IsNotFound(err) {
						next, err = s.InstallPlanNotFound(i.now(), subClient)
					}

					break
				}

				next, err = s.CheckInstallPlanStatus(i.now(), subClient, &plan.Status)
			case InstallPlanState:
				next = s.CheckReference()
			case SubscriptionExistsState:
				if s == nil {
					err = errors.New("nil state")
					break
				}
				if s.Subscription() == nil {
					err = errors.New("nil subscription in state")
					break
				}

				// Set up fresh state
				next = newInstallPlanState(s)
			default:
				// Ignore all other typestates
				utilruntime.HandleError(fmt.Errorf("unexpected subscription state in installplan reconciler %T", next))
				next = s
			}
		}
	}

	out = next

	return
}
