// Command buckety is the buckety-controller binary. It wires
// a controller-runtime manager with the Buckety + BucketyAccess
// reconcilers and the validating webhook described in SPEC.md
// §Driver interface / §Open implementation choices.
//
// CLI:
//
//	buckety -c <dir>                 controller config directory
//	buckety --periodic-recheck=5m    drift re-check cadence
//	buckety --metrics-addr=:8080     prometheus metrics endpoint
//	buckety --health-addr=:8081      healthz/readyz endpoint
//	buckety --leader-elect=true      leader election for HA
//	buckety --version                print binary + driver versions and exit
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap/zapcore"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
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
	_ "github.com/Yolean/buckety-controller/pkg/drivers/kadm"
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
		configDir       string
		periodicRecheck time.Duration
		metricsAddr     string
		healthAddr      string
		enableWebhook   bool
		webhookPort     int
		certDir         string
		leaderElect     bool
		printVersion    bool
	)
	flag.StringVar(&configDir, "c", "/etc/buckety", "directory containing buckety-controller.yaml")
	flag.DurationVar(&periodicRecheck, "periodic-recheck", 5*time.Minute, "how often to re-list resources for drift detection")
	flag.StringVar(&metricsAddr, "metrics-addr", ":8080", "metrics endpoint bind address")
	flag.StringVar(&healthAddr, "health-addr", ":8081", "healthz/readyz endpoint bind address")
	flag.BoolVar(&enableWebhook, "enable-webhook", true, "run the validating admission webhook server; set --enable-webhook=false on clusters without cert-manager (also drop deploy/kustomize/controller/webhook.yaml from the overlay). With webhook disabled, per-driver parameter validation moves to the reconcile loop and surfaces on the resource's status instead of failing the apply.")
	flag.IntVar(&webhookPort, "webhook-port", 9443, "validating webhook port (ignored when --enable-webhook=false)")
	flag.StringVar(&certDir, "cert-dir", "/var/run/buckety/webhook-tls", "directory containing tls.crt + tls.key for the webhook server (ignored when --enable-webhook=false)")
	flag.BoolVar(&leaderElect, "leader-elect", true, "enable leader election so a single replica is active during rollouts")
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

	mgrOpts := ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: healthAddr,
		LeaderElection:         leaderElect,
		LeaderElectionID:       "buckety-controller-leader",
	}
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

	requeue := func() ctrl.Result { return ctrl.Result{RequeueAfter: periodicRecheck} }

	if err := (&bucketyctrl.Reconciler{
		Client:       mgr.GetClient(),
		Scheme:       mgr.GetScheme(),
		Config:       loaded,
		RequeueAfter: requeue,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "buckety controller setup failed")
		os.Exit(1)
	}
	if err := (&accessctrl.Reconciler{
		Client:       mgr.GetClient(),
		Scheme:       mgr.GetScheme(),
		Config:       loaded,
		RequeueAfter: requeue,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "bucketyaccess controller setup failed")
		os.Exit(1)
	}

	if enableWebhook {
		(&bkhook.Validator{Config: loaded}).Register(mgr)
	} else {
		setupLog.Info("webhook disabled; per-driver parameter validation will surface on resource status only")
	}

	if err := mgr.AddHealthzCheck("ping", healthz.Ping); err != nil {
		setupLog.Error(err, "healthz registration failed")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("ping", healthz.Ping); err != nil {
		setupLog.Error(err, "readyz registration failed")
		os.Exit(1)
	}

	setupLog.Info("starting", "version", version, "drivers", driverVersions())
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "manager Start returned")
		os.Exit(1)
	}
}

func backendNames(l *config.Loaded) []string {
	out := make([]string, 0, len(l.Backends))
	for k := range l.Backends {
		out = append(out, k)
	}
	return out
}

func driverVersions() map[string]string {
	out := map[string]string{}
	for _, name := range registry.Names() {
		// Look up a no-op instance just to read its version is
		// awkward; drivers do not (yet) expose their versions
		// from the factory. For now we surface the registered
		// names and leave per-driver versions to the controller
		// logs each reconcile prints. This wires the
		// startup-time observability without an interface change.
		out[name] = "registered"
	}
	return out
}

func printVersions() {
	fmt.Printf("buckety %s\n", version)
	for _, name := range registry.Names() {
		fmt.Printf("driver %s\n", name)
	}
}
