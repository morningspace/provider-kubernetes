package object

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	"github.com/crossplane/crossplane-runtime/pkg/logging"
	"github.com/crossplane/crossplane-runtime/pkg/resource"
	"github.com/crossplane/crossplane-runtime/pkg/test"

	"github.com/crossplane-contrib/provider-kubernetes/apis/object/v1alpha1"
	kubernetesv1alpha1 "github.com/crossplane-contrib/provider-kubernetes/apis/v1alpha1"
)

const (
	providerName            = "kubernetes-test"
	providerSecretName      = "kubernetes-test-secret"
	providerSecretNamespace = "kubernetes-test-secret-namespace"

	providerSecretKey  = "kubeconfig"
	providerSecretData = "somethingsecret"

	testReleaseName = "test-release"
	testNamespace   = "test-namespace"
)

var (
	errBoom = errors.New("boom")
)

type notKubernetesObject struct {
	resource.Managed
}

type kubernetesObjectModifier func(obj *v1alpha1.Object)

func kubernetesObject(om ...kubernetesObjectModifier) *v1alpha1.Object {
	o := &v1alpha1.Object{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testReleaseName,
			Namespace: testNamespace,
		},
		Spec: v1alpha1.ObjectSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{
					Name: providerName,
				},
			},
			ForProvider: v1alpha1.ObjectParameters{
				Manifest: runtime.RawExtension{Raw: []byte(`{
				    "apiVersion": "v1",
				    "kind": "Namespace",
				    "metadata": {
				        "name": "crossplane-system",
				    }
				}`)},
			},
		},
		Status: v1alpha1.ObjectStatus{},
	}

	for _, m := range om {
		m(o)
	}

	return o
}

type providerConfigModifier func(pc *kubernetesv1alpha1.ProviderConfig)

func providerConfig(pm ...providerConfigModifier) *kubernetesv1alpha1.ProviderConfig {
	pc := &kubernetesv1alpha1.ProviderConfig{
		ObjectMeta: metav1.ObjectMeta{Name: providerName},
		Spec: kubernetesv1alpha1.ProviderConfigSpec{
			Credentials: kubernetesv1alpha1.ProviderCredentials{
				Source: xpv1.CredentialsSourceSecret,
				CommonCredentialSelectors: xpv1.CommonCredentialSelectors{
					SecretRef: &xpv1.SecretKeySelector{
						SecretReference: xpv1.SecretReference{
							Name:      providerSecretName,
							Namespace: providerSecretNamespace,
						},
						Key: providerSecretKey,
					},
				},
			},
		},
	}

	for _, m := range pm {
		m(pc)
	}

	return pc
}

func Test_connector_Connect(t *testing.T) {
	secret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: providerSecretNamespace, Name: providerSecretName},
		Data:       map[string][]byte{providerSecretKey: []byte(providerSecretData)},
	}

	type args struct {
		client          client.Client
		newRestConfigFn func(kubeconfig []byte) (*rest.Config, error)
		newKubeClientFn func(config *rest.Config) (client.Client, error)
		usage           resource.Tracker
		mg              resource.Managed
	}
	type want struct {
		err error
	}
	cases := map[string]struct {
		args
		want
	}{
		"NotReleaseResource": {
			args: args{
				mg: notKubernetesObject{},
			},
			want: want{
				err: errors.New(errNotKubernetesObject),
			},
		},
		"FailedToTrackUsage": {
			args: args{
				usage: resource.TrackerFn(func(ctx context.Context, mg resource.Managed) error { return errBoom }),
				mg:    kubernetesObject(),
			},
			want: want{
				err: errors.Wrap(errBoom, errTrackPCUsage),
			},
		},
		"FailedToGetProvider": {
			args: args{
				client: &test.MockClient{
					MockGet: func(ctx context.Context, key client.ObjectKey, obj client.Object) error {
						if key.Name == providerName {
							*obj.(*kubernetesv1alpha1.ProviderConfig) = *providerConfig()
							return errBoom
						}
						return nil
					},
				},
				usage: resource.TrackerFn(func(ctx context.Context, mg resource.Managed) error { return nil }),
				mg:    kubernetesObject(),
			},
			want: want{
				err: errors.Wrap(errBoom, errGetPC),
			},
		},
		"UnsupportedCredentialSource": {
			args: args{
				client: &test.MockClient{
					MockGet: func(ctx context.Context, key client.ObjectKey, obj client.Object) error {
						if key.Name == providerName {
							pc := providerConfig().DeepCopy()
							pc.Spec.Credentials.Source = "non-existing-source"
							*obj.(*kubernetesv1alpha1.ProviderConfig) = *pc
							return nil
						}
						return nil
					},
				},
				usage: resource.TrackerFn(func(ctx context.Context, mg resource.Managed) error { return nil }),
				mg:    kubernetesObject(),
			},
			want: want{
				err: errors.Wrap(errors.Errorf("no extraction handler registered for source: non-existing-source"), errGetCreds),
			},
		},
		"NoSecretRef": {
			args: args{
				client: &test.MockClient{
					MockGet: func(ctx context.Context, key client.ObjectKey, obj client.Object) error {
						if key.Name == providerName {
							pc := providerConfig().DeepCopy()
							pc.Spec.Credentials.SecretRef = nil
							*obj.(*kubernetesv1alpha1.ProviderConfig) = *pc
							return nil
						}
						return nil
					},
				},
				usage: resource.TrackerFn(func(ctx context.Context, mg resource.Managed) error { return nil }),
				mg:    kubernetesObject(),
			},
			want: want{
				err: errors.Wrap(errors.Errorf("cannot extract from secret key when none specified"), errGetCreds),
			},
		},
		"FailedToGetProviderSecret": {
			args: args{
				client: &test.MockClient{
					MockGet: func(ctx context.Context, key client.ObjectKey, obj client.Object) error {
						if key.Name == providerName {
							*obj.(*kubernetesv1alpha1.ProviderConfig) = *providerConfig()
							return nil
						}
						if key.Name == providerSecretName && key.Namespace == providerSecretNamespace {
							return errBoom
						}
						return errBoom
					},
				},
				usage: resource.TrackerFn(func(ctx context.Context, mg resource.Managed) error { return nil }),
				mg:    kubernetesObject(),
			},
			want: want{
				err: errors.Wrap(errors.Wrap(errBoom, "cannot get credentials secret"), errGetCreds),
			},
		},
		"FailedToCreateRestConfig": {
			args: args{
				client: &test.MockClient{
					MockGet: func(ctx context.Context, key client.ObjectKey, obj client.Object) error {
						if key.Name == providerName {
							*obj.(*kubernetesv1alpha1.ProviderConfig) = *providerConfig()
							return nil
						}
						if key.Name == providerSecretName && key.Namespace == providerSecretNamespace {
							*obj.(*corev1.Secret) = secret
							return nil
						}
						return errBoom
					},
				},
				newRestConfigFn: func(kubeconfig []byte) (config *rest.Config, err error) {
					return nil, errBoom
				},
				usage: resource.TrackerFn(func(ctx context.Context, mg resource.Managed) error { return nil }),
				mg:    kubernetesObject(),
			},
			want: want{
				err: errors.Wrap(errBoom, errFailedToCreateRestConfig),
			},
		},
		"FailedToCreateNewKubernetesClient": {
			args: args{
				client: &test.MockClient{
					MockGet: func(ctx context.Context, key client.ObjectKey, obj client.Object) error {
						if key.Name == providerName {
							*obj.(*kubernetesv1alpha1.ProviderConfig) = *providerConfig()
							return nil
						}
						if key.Name == providerSecretName && key.Namespace == providerSecretNamespace {
							*obj.(*corev1.Secret) = secret
							return nil
						}
						return errBoom
					},
					MockStatusUpdate: func(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
						return nil
					},
				},
				newRestConfigFn: func(kubeconfig []byte) (config *rest.Config, err error) {
					return &rest.Config{}, nil
				},
				newKubeClientFn: func(config *rest.Config) (c client.Client, err error) {
					return nil, errBoom
				},
				usage: resource.TrackerFn(func(ctx context.Context, mg resource.Managed) error { return nil }),
				mg:    kubernetesObject(),
			},
			want: want{
				err: errors.Wrap(errBoom, errNewKubernetesClient),
			},
		},
		"Success": {
			args: args{
				client: &test.MockClient{
					MockGet: func(ctx context.Context, key client.ObjectKey, obj client.Object) error {
						switch t := obj.(type) {
						case *kubernetesv1alpha1.ProviderConfig:
							*t = *providerConfig()
						case *corev1.Secret:
							*t = secret
						default:
							return errBoom
						}
						return nil
					},
					MockStatusUpdate: func(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
						return nil
					},
				},
				newRestConfigFn: func(kubeconfig []byte) (config *rest.Config, err error) {
					return &rest.Config{}, nil
				},
				newKubeClientFn: func(config *rest.Config) (c client.Client, err error) {
					return &test.MockClient{}, nil
				},
				usage: resource.TrackerFn(func(ctx context.Context, mg resource.Managed) error { return nil }),
				mg:    kubernetesObject(),
			},
			want: want{
				err: nil,
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			c := &connector{
				logger:          logging.NewNopLogger(),
				kube:            tc.args.client,
				newRestConfigFn: tc.args.newRestConfigFn,
				newKubeClientFn: tc.args.newKubeClientFn,
				usage:           tc.usage,
			}
			_, gotErr := c.Connect(context.Background(), tc.args.mg)
			if diff := cmp.Diff(tc.want.err, gotErr, test.EquateErrors()); diff != "" {
				t.Fatalf("Connect(...): -want error, +got error: %s", diff)
			}
		})
	}
}
