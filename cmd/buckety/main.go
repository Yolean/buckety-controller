// Command buckety is the buckety-controller binary. It wires
// a controller-runtime manager with the Buckety + BucketyAccess
// reconcilers and the validating webhook described in SPEC.md
// §Driver interface / §Open implementation choices.
//
// CLI:
//
//	buckety -c <dir>                 controller config directory
//	buckety --periodic-recheck=5m    drift re-check cadence
//	buckety --reconcile-timeout=2m   deadline of one reconcile pass
//	buckety --metrics-addr=:8080     prometheus metrics endpoint
//	buckety --health-addr=:8081      healthz/readyz endpoint
//	buckety --leader-elect=true      leader election for HA
//	buckety --leader-elect-lease-duration=60s --leader-elect-renew-deadline=40s --leader-elect-retry-period=15s
//	buckety --version                print binary + driver versions and exit
package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	"go.uber.org/zap/zapcore"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	bucketyv1 "github.com/Yolean/buckety-controller/pkg/api/v1alpha1"
	"github.com/Yolean/buckety-controller/pkg/config"
	bucketyctrl "github.com/Yolean/buckety-controller/pkg/controller/buckety"
	accessctrl "github.com/Yolean/buckety-controller/pkg/controller/bucketyaccess"
	"github.com/Yolean/buckety-controller/pkg/drivers/registry"
	bkhook "github.com/Yolean/buckety-controller/pkg/webhook"

	// Side-effect imports register driver factories with
	// pkg/drivers/registry. The binary's driver set is exactly
	// this import list.
	_ "github.com/Yolean/buckety-controller/pkg/drivers/gcs"
	_ "github.com/Yolean/buckety-controller/pkg/drivers/kadm"
	_ "github.com/Yolean/buckety-controller/pkg/drivers/mysql"
	_ "github.com/Yolean/buckety-controller/pkg/drivers/s3"
)

// version is stamped at build time via
//
//	-ldflags '-X main.version=...'
//
// per contain.yaml's example. Driver versions live on their own
// package variables (see pkg/drivers/<driver>/driver.go).
var version = "dev"

func main() {
	var (
		configDir        string
		periodicRecheck  time.Duration
		reconcileTimeout time.Duration
		metricsAddr      string
		healthAddr       string
		enableWebhook    bool
		webhookPort      int
		certDir          string
		leaderElect      bool
		lease            leaseTiming
		printVersion     bool
	)
	flag.StringVar(&configDir, "c", "/etc/buckety", "directory containing buckety-controller.yaml")
	flag.DurationVar(&periodicRecheck, "periodic-recheck", 5*time.Minute, "how often to re-list resources for drift detection")
	flag.DurationVar(&reconcileTimeout, "reconcile-timeout", defaultReconcileTimeout, "deadline of one reconcile pass, backend calls included; a pass that exceeds it fails with the context error and is retried with backoff, so one unresponsive backend cannot hold the controller's workers indefinitely (0 disables)")
	flag.StringVar(&metricsAddr, "metrics-addr", ":8080", "metrics endpoint bind address")
	flag.StringVar(&healthAddr, "health-addr", ":8081", "healthz/readyz endpoint bind address")
	flag.BoolVar(&enableWebhook, "enable-webhook", true, "run the validating admission webhook server; set --enable-webhook=false on clusters without cert-manager (also drop deploy/kustomize/controller/webhook.yaml from the overlay). With webhook disabled, per-driver parameter validation moves to the reconcile loop and surfaces on the resource's status instead of failing the apply.")
	flag.IntVar(&webhookPort, "webhook-port", 9443, "validating webhook port (ignored when --enable-webhook=false)")
	flag.StringVar(&certDir, "cert-dir", "/var/run/buckety/webhook-tls", "directory containing tls.crt + tls.key for the webhook server (ignored when --enable-webhook=false)")
	flag.BoolVar(&leaderElect, "leader-elect", true, "enable leader election so a single replica is active during rollouts")
	flag.DurationVar(&lease.duration, "leader-elect-lease-duration", defaultLease.duration, "how long a lease the leader stopped renewing blocks another replica from taking over")
	flag.DurationVar(&lease.renewDeadline, "leader-elect-renew-deadline", defaultLease.renewDeadline, "how long the leader keeps retrying a failed lease renewal before it gives up leadership (and the process exits); each request may take half of it")
	flag.DurationVar(&lease.retryPeriod, "leader-elect-retry-period", defaultLease.retryPeriod, "how often the leader renews its lease, and how often a candidate tries to acquire it")
	flag.BoolVar(&printVersion, "version", false, "print binary + driver versions and exit")
	flag.Parse()

	if printVersion {
		printVersions()
		return
	}

	logger := zap.New(zap.UseDevMode(false), zap.Level(zapcore.InfoLevel))
	ctrl.SetLogger(logger)
	setupLog := ctrl.Log.WithName("setup")

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(bucketyv1.AddToScheme(scheme))

	setupLog.Info("loading config", "dir", configDir)
	loaded, err := config.Load(configDir)
	if err != nil {
		setupLog.Error(err, "config load failed")
		os.Exit(1)
	}
	setupLog.Info("config loaded", "backends", backendNames(loaded))

	mgrOpts := managerOptions(scheme, metricsAddr, healthAddr, leaderElect, lease, reconcileTimeout)
	if enableWebhook {
		mgrOpts.WebhookServer = webhook.NewServer(webhook.Options{
			Port:    webhookPort,
			CertDir: certDir,
		})
	}
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), mgrOpts)
	if err != nil {
		setupLog.Error(err, "manager construction failed")
		os.Exit(1)
	}

	if err := (&bucketyctrl.Reconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Config:   loaded,
		Recheck:  periodicRecheck,
		Recorder: mgr.GetEventRecorderFor("buckety-controller"),
		Live:     mgr.GetAPIReader(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "buckety controller setup failed")
		os.Exit(1)
	}
	if err := (&accessctrl.Reconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Config:   loaded,
		Recheck:  periodicRecheck,
		Recorder: mgr.GetEventRecorderFor("buckety-controller"),
		Live:     mgr.GetAPIReader(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "bucketyaccess controller setup failed")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("ping", healthz.Ping); err != nil {
		setupLog.Error(err, "healthz registration failed")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("ping", healthz.Ping); err != nil {
		setupLog.Error(err, "readyz registration failed")
		os.Exit(1)
	}
	if enableWebhook {
		(&bkhook.Validator{Config: loaded}).Register(mgr)
		// Ready only once the webhook listener answers. The
		// ValidatingWebhookConfiguration has failurePolicy Fail, so
		// a Pod that is Ready - and therefore in the Service's
		// endpoints - before its TLS listener is up makes every
		// Buckety apply during a rollout fail with "failed calling
		// webhook".
		if err := mgr.AddReadyzCheck("webhook", mgr.GetWebhookServer().StartedChecker()); err != nil {
			setupLog.Error(err, "webhook readyz registration failed")
			os.Exit(1)
		}
	} else {
		setupLog.Info("webhook disabled; per-driver parameter validation will surface on resource status only")
	}

	setupLog.Info("starting", "version", version, "drivers", registry.Versions())
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "manager Start returned")
		os.Exit(1)
	}
}

// defaultReconcileTimeout bounds one reconcile pass. The slowest
// legitimate pass (a kadm alter riding out its client retries, a
// mysql password change verified against a replicating cluster,
// one teardown slice) takes seconds; minutes mean a backend that
// stopped answering.
const defaultReconcileTimeout = 2 * time.Minute

// leaseTiming is the leader election lease timing.
type leaseTiming struct {
	duration, renewDeadline, retryPeriod time.Duration
}

// defaultLease tolerates a slow apiserver. controller-runtime's
// defaults (15s lease, 10s renew deadline, 2s retry period, so 5s
// per request) end leadership, and with it the process, when the
// apiserver answers one lease update slower than 5s twice in a row:
// a single-node cluster under load does that, and each loss
// restarts the controller and re-runs every reconcile. Longer
// timing only delays a takeover after a leader dies without
// releasing (graceful stops release, see managerOptions), and the
// lease is renewed every 15s instead of every 2s.
var defaultLease = leaseTiming{
	duration:      60 * time.Second,
	renewDeadline: 40 * time.Second,
	retryPeriod:   15 * time.Second,
}

// managerOptions are the manager's options from the command line,
// without the webhook server.
func managerOptions(scheme *runtime.Scheme, metricsAddr, healthAddr string, leaderElect bool, lease leaseTiming, reconcileTimeout time.Duration) ctrl.Options {
	return ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: healthAddr,
		LeaderElection:         leaderElect,
		LeaderElectionID:       "buckety-controller-leader",
		LeaseDuration:          &lease.duration,
		RenewDeadline:          &lease.renewDeadline,
		RetryPeriod:            &lease.retryPeriod,
		// main returns right after the manager stops, which is what
		// releasing on cancel requires: a stopped leader frees the
		// lease, and its successor (the Deployment recreates the
		// Pod) starts at once instead of waiting out the lease.
		LeaderElectionReleaseOnCancel: true,
		// Every controller of the manager: a pass whose backend
		// stops answering fails with the context error after
		// reconcileTimeout instead of holding a worker - with one
		// worker per controller, every other object - until the
		// backend comes back. The failure is retried with the
		// workqueue's backoff and surfaces on status.
		Controller: ctrlconfig.Controller{ReconciliationTimeout: reconcileTimeout},
		// The Secret informer is scoped to controller-minted
		// Secrets; unscoped, the Owns() watch LISTs and caches
		// every Secret in the cluster, and controller memory
		// tracks cluster size instead of buckety usage (OOMKilled
		// at default limits on a multi-site cluster, issue #10).
		// Reads that must see unlabelled Secrets go through
		// mgr.GetAPIReader() (see the access reconciler's Live).
		Cache: cache.Options{
			ByObject: map[client.Object]cache.ByObject{
				&corev1.Secret{}: {Label: labels.SelectorFromSet(labels.Set{
					bucketyv1.LabelOwnedSecret: "true",
				})},
			},
		},
	}
}

func backendNames(l *config.Loaded) []string {
	out := make([]string, 0, len(l.Backends))
	for k := range l.Backends {
		out = append(out, k)
	}
	return out
}

func printVersions() {
	fmt.Printf("buckety %s\n", version)
	versions := registry.Versions()
	names := registry.Names()
	sort.Strings(names)
	for _, name := range names {
		fmt.Printf("driver %s %s\n", name, versions[name])
	}
}
