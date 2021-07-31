/*
Copyright 2021 The Crossplane Authors.

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

package object

import (
	"context"
	"time"

	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	"github.com/crossplane/crossplane-runtime/pkg/event"
	"github.com/crossplane/crossplane-runtime/pkg/logging"
	"github.com/crossplane/crossplane-runtime/pkg/meta"
	"github.com/crossplane/crossplane-runtime/pkg/ratelimiter"
	"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/pkg/resource"

	"github.com/crossplane-contrib/provider-kubernetes/apis/object/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-kubernetes/apis/v1alpha1"
	"github.com/crossplane-contrib/provider-kubernetes/internal/clients"
)

type ManagementType = string
type FinalizerOp = string

const (
	Default                ManagementType = "Default"
	Undeletable            ManagementType = "Undeletable"
	ObservableAndDeletable ManagementType = "ObservableAndDeletable"
	Observable             ManagementType = "Observable"

	Add    FinalizerOp = "Add"
	Remove FinalizerOp = "Remove"

	annoManagementType = "kubernetes.crossplane.io/managementType"
	finalizerPrefix    = "finalizer.kubernetes.crossplane.io"

	errTrackPCUsage = "cannot track ProviderConfig usage"
	errGetPC        = "cannot get ProviderConfig"
	errGetCreds     = "cannot get credentials"
	errGetObject    = "cannot get object"
	errCreateObject = "cannot create object"
	errApplyObject  = "cannot apply object"
	errDeleteObject = "cannot delete object"

	errNotKubernetesObject      = "managed resource is not an Object custom resource"
	errNewKubernetesClient      = "cannot create new Kubernetes client"
	errFailedToCreateRestConfig = "cannot create new rest config using provider secret"

	errGetLastApplied          = "cannot get last applied"
	errUnmarshalTemplate       = "cannot unmarshal template"
	errFailedToMarshalExisting = "cannot marshal existing resource"

	errFailedToResolveResourceRefs = "cannot resolve resource references"
)

// Setup adds a controller that reconciles Object managed resources.
func Setup(mgr ctrl.Manager, l logging.Logger, rl workqueue.RateLimiter, poll time.Duration) error {
	name := managed.ControllerName(v1alpha1.ObjectGroupKind)

	logger := l.WithValues("controller", name)

	o := controller.Options{
		RateLimiter: ratelimiter.NewDefaultManagedRateLimiter(rl),
	}

	r := managed.NewReconciler(mgr,
		resource.ManagedKind(v1alpha1.ObjectGroupVersionKind),
		managed.WithExternalConnecter(&connector{
			logger:          logger,
			kube:            mgr.GetClient(),
			usage:           resource.NewProviderConfigUsageTracker(mgr.GetClient(), &apisv1alpha1.ProviderConfigUsage{}),
			newRestConfigFn: clients.NewRestConfig,
			newKubeClientFn: clients.NewKubeClient,
		}),
		managed.WithLogger(logger),
		managed.WithPollInterval(poll),
		managed.WithRecorder(event.NewAPIRecorder(mgr.GetEventRecorderFor(name))))

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o).
		For(&v1alpha1.Object{}).
		Complete(r)
}

type connector struct {
	kube            client.Client
	usage           resource.Tracker
	logger          logging.Logger
	newRestConfigFn func(kubeconfig []byte) (*rest.Config, error)
	newKubeClientFn func(config *rest.Config) (client.Client, error)
}

func (c *connector) Connect(ctx context.Context, mg resource.Managed) (managed.ExternalClient, error) {
	cr, ok := mg.(*v1alpha1.Object)
	if !ok {
		return nil, errors.New(errNotKubernetesObject)
	}

	if err := c.usage.Track(ctx, mg); err != nil {
		return nil, errors.Wrap(err, errTrackPCUsage)
	}

	pc := &apisv1alpha1.ProviderConfig{}
	if err := c.kube.Get(ctx, types.NamespacedName{Name: cr.GetProviderConfigReference().Name}, pc); err != nil {
		return nil, errors.Wrap(err, errGetPC)
	}

	var rc *rest.Config
	var err error
	cd := pc.Spec.Credentials

	if cd.Source == xpv1.CredentialsSourceInjectedIdentity {
		rc, err = rest.InClusterConfig()
		if err != nil {
			return nil, errors.Wrap(err, errFailedToCreateRestConfig)
		}
	} else {
		var kc []byte
		if kc, err = resource.CommonCredentialExtractor(ctx, cd.Source, c.kube, cd.CommonCredentialSelectors); err != nil {
			return nil, errors.Wrap(err, errGetCreds)
		}

		if rc, err = c.newRestConfigFn(kc); err != nil {
			return nil, errors.Wrap(err, errFailedToCreateRestConfig)
		}
	}

	k, err := c.newKubeClientFn(rc)
	if err != nil {
		return nil, errors.Wrap(err, errNewKubernetesClient)
	}

	return &external{
		logger: c.logger,
		client: resource.ClientApplicator{
			Client:     k,
			Applicator: resource.NewAPIPatchingApplicator(k),
		},
	}, nil
}

type external struct {
	logger logging.Logger
	client resource.ClientApplicator
}

func (c *external) ResolveReferencies(ctx context.Context, obj *v1alpha1.Object) error {
	for _, ref := range obj.Spec.References {
		// Try to get referenced resource
		res := &unstructured.Unstructured{}
		res.SetAPIVersion(ref.FromObject.APIVersion)
		res.SetKind(ref.FromObject.Kind)
		err := c.client.Get(ctx, client.ObjectKey{
			Namespace: ref.FromObject.Namespace,
			Name:      ref.FromObject.Name,
		}, res)

		if err != nil {
			return errors.Wrap(err, "failed to get referenced resource")
		}

		// Retrieve value from FieldPath and apply to ToFieldPath
		if err := ref.ApplyFromFieldPathPatch(res, obj); err != nil {
			return errors.Wrap(err, "failed to patch from referenced resource")
		}
	}

	return nil
}

func (c *external) ManageReferenceFinalizer(ctx context.Context, obj *v1alpha1.Object, op FinalizerOp) error {
	f := finalizerPrefix + "/" + obj.ObjectMeta.Name
	for _, ref := range obj.Spec.References {
		var refRes *unstructured.Unstructured
		if ref.FromObject.Kind == "Object" && ref.FromObject.APIVersion == "kubernetes.crossplane.io/v1alpha1" {
			// The referenced resource is an Object.
			// Resolve the referenced resource managed by the Object
			refObj := &v1alpha1.Object{}
			err := c.client.Get(ctx, client.ObjectKey{
				Namespace: ref.FromObject.Namespace,
				Name:      ref.FromObject.Name,
			}, refObj)

			if err != nil {
				c.logger.Debug("Failed to get referenced Object.", err)
				continue
			}

			desiredRes, err := getDesired(refObj)
			if err != nil {
				c.logger.Debug("Failed to get referenced resource.", err)
				continue
			}

			refRes = desiredRes.DeepCopy()

			err = c.client.Get(ctx, types.NamespacedName{
				Namespace: refRes.GetNamespace(),
				Name:      refRes.GetName(),
			}, refRes)

			if err != nil {
				c.logger.Debug("Failed to get referenced resource.", err)
				continue
			}
		} else {
			// Resolve the referenced resource
			refRes = &unstructured.Unstructured{}
			refRes.SetAPIVersion(ref.FromObject.APIVersion)
			refRes.SetKind(ref.FromObject.Kind)
			err := c.client.Get(ctx, client.ObjectKey{
				Namespace: ref.FromObject.Namespace,
				Name:      ref.FromObject.Name,
			}, refRes)

			if err != nil {
				c.logger.Debug("Failed to get referenced resource.", err)
				continue
			}
		}

		if op == "Add" {
			if !meta.FinalizerExists(refRes, f) {
				meta.AddFinalizer(refRes, f)
				if err := c.client.Apply(ctx, refRes); err != nil {
					c.logger.Debug("Failed to add finalizer to referenced resource.", err)
					continue
				}
			}
		} else {
			if meta.FinalizerExists(refRes, f) {
				meta.RemoveFinalizer(refRes, f)
				if err := c.client.Apply(ctx, refRes); err != nil {
					c.logger.Debug("Failed to remove finalizer from referenced resource.", err)
					continue
				}
			}
		}
	}

	return nil
}

func (c *external) Observe(ctx context.Context, mg resource.Managed) (managed.ExternalObservation, error) {
	cr, ok := mg.(*v1alpha1.Object)
	if !ok {
		return managed.ExternalObservation{}, errors.New(errNotKubernetesObject)
	}

	c.logger.Debug("Observing", "resource", cr)

	// See if the managed resource is being deleted while the corresponding external resource is undeletable.
	// In such a case, we should detach from the external resource to allow the managed resource gets deleted.
	// As a result, the external resource will get back to be unmanaged.
	mt, ok := cr.GetAnnotations()[annoManagementType]
	if meta.WasDeleted(cr) {
		if ok && (mt == Undeletable || mt == Observable) {
			c.logger.Debug("Managed resource being deleted but external resource undeletable, detaching from it.")
			return managed.ExternalObservation{ResourceExists: false}, nil
		}
	}

	// Resolve reference if there is any
	// When failed, e.g. due to reference not ready yet, throw error and requeue to resolve it later
	if err := c.ResolveReferencies(ctx, cr); err != nil {
		return managed.ExternalObservation{}, errors.Wrap(err, errFailedToResolveResourceRefs)
	}

	c.ManageReferenceFinalizer(ctx, cr, Add)

	desired, err := getDesired(cr)
	if err != nil {
		return managed.ExternalObservation{}, err
	}

	observed := desired.DeepCopy()

	err = c.client.Get(ctx, types.NamespacedName{
		Namespace: observed.GetNamespace(),
		Name:      observed.GetName(),
	}, observed)

	if kerrors.IsNotFound(err) {
		if meta.WasDeleted(cr) {
			c.ManageReferenceFinalizer(ctx, cr, Remove)
		}
		return managed.ExternalObservation{ResourceExists: false}, nil
	}
	if err != nil {
		return managed.ExternalObservation{}, errors.Wrap(err, errGetObject)
	}

	if err = setObserved(cr, observed); err != nil {
		return managed.ExternalObservation{}, err
	}

	var last *unstructured.Unstructured
	if last, err = getLastApplied(cr, observed); err != nil {
		return managed.ExternalObservation{}, errors.Wrap(err, errGetLastApplied)
	}
	if last == nil {
		if mt == "observable_and_deletable" || mt == "observable" {
			c.logger.Debug("Managed resource observable, skip updating last-applied-configuration.")
			// Set condition as available
			cr.Status.SetConditions(xpv1.Available())
			return managed.ExternalObservation{
				ResourceExists:   true,
				ResourceUpToDate: true,
			}, nil
		} else {
			return managed.ExternalObservation{
				ResourceExists:   true,
				ResourceUpToDate: false,
			}, nil
		}
	}

	if equality.Semantic.DeepEqual(last, desired) {
		c.logger.Debug("Up to date!")
		// Set condition as available
		cr.Status.SetConditions(xpv1.Available())
		return managed.ExternalObservation{
			ResourceExists:   true,
			ResourceUpToDate: true,
		}, nil
	}

	return managed.ExternalObservation{
		ResourceExists:   true,
		ResourceUpToDate: false,
	}, nil
}

func (c *external) Create(ctx context.Context, mg resource.Managed) (managed.ExternalCreation, error) {
	cr, ok := mg.(*v1alpha1.Object)
	if !ok {
		return managed.ExternalCreation{}, errors.New(errNotKubernetesObject)
	}

	c.logger.Debug("Creating", "resource", cr)

	// See if the external resource is defined as observable. If that is the case,
	// we do not create the external resource.
	if mt, ok := cr.GetAnnotations()[annoManagementType]; ok &&
		(mt == ObservableAndDeletable || mt == Observable) {
		c.logger.Debug("Managed resource observable, skip creating.")
		return managed.ExternalCreation{}, nil
	}

	obj, err := getDesired(cr)
	if err != nil {
		return managed.ExternalCreation{}, err
	}

	meta.AddAnnotations(obj, map[string]string{
		v1.LastAppliedConfigAnnotation: string(cr.Spec.ForProvider.Manifest.Raw),
	})

	if err := c.client.Create(ctx, obj); err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateObject)
	}

	cr.Status.SetConditions(xpv1.Available())
	return managed.ExternalCreation{}, setObserved(cr, obj)
}

func (c *external) Update(ctx context.Context, mg resource.Managed) (managed.ExternalUpdate, error) {
	cr, ok := mg.(*v1alpha1.Object)
	if !ok {
		return managed.ExternalUpdate{}, errors.New(errNotKubernetesObject)
	}

	c.logger.Debug("Updating", "resource", cr)

	// See if the external resource is defined as observable. If that is the case,
	// we do not update the external resource.
	if mt, ok := cr.GetAnnotations()[annoManagementType]; ok &&
		(mt == ObservableAndDeletable || mt == Observable) {
		c.logger.Debug("Managed resource observable, skip updating.")
		return managed.ExternalUpdate{}, nil
	}

	obj, err := getDesired(cr)
	if err != nil {
		return managed.ExternalUpdate{}, err
	}

	meta.AddAnnotations(obj, map[string]string{
		v1.LastAppliedConfigAnnotation: string(cr.Spec.ForProvider.Manifest.Raw),
	})

	if err := c.client.Apply(ctx, obj); err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errApplyObject)
	}

	cr.Status.SetConditions(xpv1.Available())
	return managed.ExternalUpdate{}, setObserved(cr, obj)
}

func (c *external) Delete(ctx context.Context, mg resource.Managed) error {
	cr, ok := mg.(*v1alpha1.Object)
	if !ok {
		return errors.New(errNotKubernetesObject)
	}

	c.logger.Debug("Deleting", "resource", cr)

	// See if the external resource is defined as observable or undeletable. If that is the case,
	// we do not delete the external resource and assume this will be handled by external system.
	if mt, ok := cr.GetAnnotations()[annoManagementType]; ok &&
		(mt == Undeletable || mt == Observable) {
		c.logger.Debug("Managed resource undeletable, skip deleting.")
		return nil
	}

	obj, err := getDesired(cr)
	if err != nil {
		return err
	}

	return errors.Wrap(resource.IgnoreNotFound(c.client.Delete(ctx, obj)), errDeleteObject)
}

func getDesired(obj *v1alpha1.Object) (*unstructured.Unstructured, error) {
	desired := &unstructured.Unstructured{}
	if err := json.Unmarshal(obj.Spec.ForProvider.Manifest.Raw, desired); err != nil {
		return nil, errors.Wrap(err, errUnmarshalTemplate)
	}

	if desired.GetName() == "" {
		desired.SetName(obj.Name)
	}
	return desired, nil
}

func getLastApplied(obj *v1alpha1.Object, observed *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	lastApplied, ok := observed.GetAnnotations()[v1.LastAppliedConfigAnnotation]
	if !ok {
		return nil, nil
	}

	last := &unstructured.Unstructured{}
	if err := json.Unmarshal([]byte(lastApplied), last); err != nil {
		return nil, errors.Wrap(err, errUnmarshalTemplate)
	}

	if last.GetName() == "" {
		last.SetName(obj.Name)
	}

	return last, nil
}

func setObserved(obj *v1alpha1.Object, observed *unstructured.Unstructured) error {
	var err error
	if obj.Status.AtProvider.Manifest.Raw, err = observed.MarshalJSON(); err != nil {
		return errors.Wrap(err, errFailedToMarshalExisting)
	}
	return nil
}
