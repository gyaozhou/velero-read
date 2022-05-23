/*
Copyright the Velero contributors.

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

package restic

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"

	kbclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/vmware-tanzu/velero/internal/credentials"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	clientset "github.com/vmware-tanzu/velero/pkg/generated/clientset/versioned"
	velerov1client "github.com/vmware-tanzu/velero/pkg/generated/clientset/versioned/typed/velero/v1"
	velerov1informers "github.com/vmware-tanzu/velero/pkg/generated/informers/externalversions/velero/v1"
	velerov1listers "github.com/vmware-tanzu/velero/pkg/generated/listers/velero/v1"
	veleroexec "github.com/vmware-tanzu/velero/pkg/util/exec"
	"github.com/vmware-tanzu/velero/pkg/util/filesystem"
)

// zhou: there is no way to delete a restic repo

// RepositoryManager executes commands against restic repositories.
type RepositoryManager interface {
	// InitRepo initializes a repo with the specified name and identifier.
	InitRepo(repo *velerov1api.BackupRepository) error

	// ConnectToRepo runs the 'restic snapshots' command against the
	// specified repo, and returns an error if it fails. This is
	// intended to be used to ensure that the repo exists/can be
	// authenticated to.
	ConnectToRepo(repo *velerov1api.BackupRepository) error

	// PruneRepo deletes unused data from a repo.
	PruneRepo(repo *velerov1api.BackupRepository) error

	// UnlockRepo removes stale locks from a repo.
	UnlockRepo(repo *velerov1api.BackupRepository) error

	// Forget removes a snapshot from the list of
	// available snapshots in a repo.
	Forget(context.Context, SnapshotIdentifier) error

	BackupperFactory

	RestorerFactory
}

// BackupperFactory can construct restic backuppers.
type BackupperFactory interface {
	// NewBackupper returns a restic backupper for use during a single
	// Velero backup.
	NewBackupper(context.Context, *velerov1api.Backup) (Backupper, error)
}

// RestorerFactory can construct restic restorers.
type RestorerFactory interface {
	// NewRestorer returns a restic restorer for use during a single
	// Velero restore.
	NewRestorer(context.Context, *velerov1api.Restore) (Restorer, error)
}

// zhou: shared within velero server

type repositoryManager struct {
	namespace            string // zhou: namespace here is for the namespce velero watchs.
	veleroClient         clientset.Interface
	repoLister           velerov1listers.BackupRepositoryLister
	repoInformerSynced   cache.InformerSynced
	kbClient             kbclient.Client
	log                  logrus.FieldLogger
	repoLocker           *repoLocker        // zhou: a RW lock, used to protect restic cli on each repo.
	repoEnsurer          *repositoryEnsurer // zhou: used to ensure the restic repo is init for specific namespace.
	fileSystem           filesystem.Interface
	ctx                  context.Context
	pvcClient            corev1client.PersistentVolumeClaimsGetter
	pvClient             corev1client.PersistentVolumesGetter
	credentialsFileStore credentials.FileStore
}

const (
	// insecureSkipTLSVerifyKey is the flag in BackupStorageLocation's config
	// to indicate whether to skip TLS verify to setup insecure HTTPS connection.
	insecureSkipTLSVerifyKey = "insecureSkipTLSVerify"

	// resticInsecureTLSFlag is the flag for Restic command line to indicate
	// skip TLS verify on https connection.
	resticInsecureTLSFlag = "--insecure-tls"
)

// zhou: used in velero server to create PodVolumeBackup/PodVolumeRestore and
//       execute "restic forget" and "restic prune" directly.

// NewRepositoryManager constructs a RepositoryManager.
func NewRepositoryManager(
	ctx context.Context,
	namespace string,
	veleroClient clientset.Interface,
	repoInformer velerov1informers.BackupRepositoryInformer,
	repoClient velerov1client.BackupRepositoriesGetter,
	kbClient kbclient.Client,
	pvcClient corev1client.PersistentVolumeClaimsGetter,
	pvClient corev1client.PersistentVolumesGetter,
	credentialFileStore credentials.FileStore,
	log logrus.FieldLogger,
) (RepositoryManager, error) {
	rm := &repositoryManager{
		namespace:            namespace,
		veleroClient:         veleroClient,
		repoLister:           repoInformer.Lister(),
		repoInformerSynced:   repoInformer.Informer().HasSynced,
		kbClient:             kbClient,
		pvcClient:            pvcClient,
		pvClient:             pvClient,
		credentialsFileStore: credentialFileStore,
		log:                  log,
		ctx:                  ctx,

		repoLocker:  newRepoLocker(),
		repoEnsurer: newRepositoryEnsurer(repoInformer, repoClient, log),
		fileSystem:  filesystem.NewFileSystem(),
	}

	return rm, nil
}

// zhou: each backup controller reconcile will create a new restic backupper.
//       Invoked by "kubernetesBackupper.BackupWithResolvers()".

func (rm *repositoryManager) NewBackupper(ctx context.Context, backup *velerov1api.Backup) (Backupper, error) {
	// zhou: get api PodVolumeBackup informer.
	//       Why need to create PodVolumeBackup informer for each backup reconcile?
	//       Because there is no shared indexer like controller-runtime did, what here need is
	//       "AddEventHandler()" to handle PodVolumeBackup events.
	//       So, there are still some reduntant logic.
	informer := velerov1informers.NewFilteredPodVolumeBackupInformer(
		rm.veleroClient,
		backup.Namespace,
		0,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
		func(opts *metav1.ListOptions) {
			opts.LabelSelector = fmt.Sprintf("%s=%s", velerov1api.BackupUIDLabel, backup.UID)
		},
	)

	// zhou: create restic backup handler, which watchs the PodVolumebBackup CR phase changes.
	b := newBackupper(ctx, rm, rm.repoEnsurer, informer, rm.pvcClient, rm.pvClient, rm.log)

	// zhou: wait for cache sync done in another thread.
	go informer.Run(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced, rm.repoInformerSynced) {
		return nil, errors.New("timed out waiting for caches to sync")
	}

	return b, nil
}

// zhou: start PodVolumeRestore Informer, and AddEventHandler fo PodVolumeRestore Informer
//       which will watch/notify CR phase becomes completed.

func (rm *repositoryManager) NewRestorer(ctx context.Context, restore *velerov1api.Restore) (Restorer, error) {
	informer := velerov1informers.NewFilteredPodVolumeRestoreInformer(
		rm.veleroClient,
		restore.Namespace,
		0,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
		func(opts *metav1.ListOptions) {
			opts.LabelSelector = fmt.Sprintf("%s=%s", velerov1api.RestoreUIDLabel, restore.UID)
		},
	)
	// zhou: AddEventHandler fo PodVolumeRestore Informer, which will watch/notify completion.
	r := newRestorer(ctx, rm, rm.repoEnsurer, informer, rm.pvcClient, rm.log)

	go informer.Run(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced, rm.repoInformerSynced) {
		return nil, errors.New("timed out waiting for cache to sync")
	}

	return r, nil
}

// zhou: init Restic repo.
//       Write lock to protect "restic -r xxx init" in ResticRepository controller.

func (rm *repositoryManager) InitRepo(repo *velerov1api.BackupRepository) error {
	// restic init requires an exclusive lock
	rm.repoLocker.LockExclusive(repo.Name)
	defer rm.repoLocker.UnlockExclusive(repo.Name)

	return rm.exec(InitCommand(repo.Spec.ResticIdentifier), repo.Spec.BackupStorageLocation)
}

// zhou: verify the connection.
//       Read lock to protet "restic -r xxx snapshots xx" in ResticRepository controller.

func (rm *repositoryManager) ConnectToRepo(repo *velerov1api.BackupRepository) error {
	// restic snapshots requires a non-exclusive lock
	rm.repoLocker.Lock(repo.Name)
	defer rm.repoLocker.Unlock(repo.Name)

	snapshotsCmd := SnapshotsCommand(repo.Spec.ResticIdentifier)
	// use the '--latest=1' flag to minimize the amount of data fetched since
	// we're just validating that the repo exists and can be authenticated
	// to.
	// "--last" is replaced by "--latest=1" in restic v0.12.1
	snapshotsCmd.ExtraFlags = append(snapshotsCmd.ExtraFlags, "--latest=1")

	return rm.exec(snapshotsCmd, repo.Spec.BackupStorageLocation)
}

// zhou: Cleanup Restic repo.
//       Write lock to protect "restic -r xxx prune" in ResticRepository controller.

func (rm *repositoryManager) PruneRepo(repo *velerov1api.BackupRepository) error {
	// restic prune requires an exclusive lock
	rm.repoLocker.LockExclusive(repo.Name)
	defer rm.repoLocker.UnlockExclusive(repo.Name)

	return rm.exec(PruneCommand(repo.Spec.ResticIdentifier), repo.Spec.BackupStorageLocation)
}

// zhou: checking stale locks in Restic repo.
//       Read lock to protect "restic -r xxx unlock" in ResticRepository controller.

func (rm *repositoryManager) UnlockRepo(repo *velerov1api.BackupRepository) error {
	// restic unlock requires a non-exclusive lock
	rm.repoLocker.Lock(repo.Name)
	defer rm.repoLocker.Unlock(repo.Name)

	return rm.exec(UnlockCommand(repo.Spec.ResticIdentifier), repo.Spec.BackupStorageLocation)
}

// zhou: delete from Restic repo
//       Write lock to protect "restic -r xxx forget" in DeleteBackupRequest controller

func (rm *repositoryManager) Forget(ctx context.Context, snapshot SnapshotIdentifier) error {
	// We can't wait for this in the constructor, because this informer is coming
	// from the shared informer factory, which isn't started until *after* the repo
	// manager is instantiated & passed to the controller constructors. We'd get a
	// deadlock if we tried to wait for this in the constructor.
	if !cache.WaitForCacheSync(ctx.Done(), rm.repoInformerSynced) {
		return errors.New("timed out waiting for cache to sync")
	}

	repo, err := rm.repoEnsurer.EnsureRepo(ctx, rm.namespace, snapshot.VolumeNamespace, snapshot.BackupStorageLocation)
	if err != nil {
		return err
	}

	// restic forget requires an exclusive lock
	rm.repoLocker.LockExclusive(repo.Name)
	defer rm.repoLocker.UnlockExclusive(repo.Name)

	return rm.exec(ForgetCommand(repo.Spec.ResticIdentifier, snapshot.SnapshotID), repo.Spec.BackupStorageLocation)
}

// zhou: execute "restic" command

func (rm *repositoryManager) exec(cmd *Command, backupLocation string) error {
	file, err := rm.credentialsFileStore.Path(RepoKeySelector())
	if err != nil {
		return err
	}
	// ignore error since there's nothing we can do and it's a temp file.
	defer os.Remove(file)

	cmd.PasswordFile = file

	loc := &velerov1api.BackupStorageLocation{}
	if err := rm.kbClient.Get(context.Background(), kbclient.ObjectKey{
		Namespace: rm.namespace,
		Name:      backupLocation,
	}, loc); err != nil {
		return errors.Wrap(err, "error getting backup storage location")
	}

	// if there's a caCert on the ObjectStorage, write it to disk so that it can be passed to restic
	var caCertFile string
	if loc.Spec.ObjectStorage != nil && loc.Spec.ObjectStorage.CACert != nil {
		caCertFile, err = TempCACertFile(loc.Spec.ObjectStorage.CACert, backupLocation, rm.fileSystem)
		if err != nil {
			return errors.Wrap(err, "error creating temp cacert file")
		}
		// ignore error since there's nothing we can do and it's a temp file.
		defer os.Remove(caCertFile)
	}
	cmd.CACertFile = caCertFile

	env, err := CmdEnv(loc, rm.credentialsFileStore)
	if err != nil {
		return err
	}
	cmd.Env = env

	// #4820: restrieve insecureSkipTLSVerify from BSL configuration for
	// AWS plugin. If nothing is return, that means insecureSkipTLSVerify
	// is not enable for Restic command.
	skipTLSRet := GetInsecureSkipTLSVerifyFromBSL(loc, rm.log)
	if len(skipTLSRet) > 0 {
		cmd.ExtraFlags = append(cmd.ExtraFlags, skipTLSRet)
	}

	stdout, stderr, err := veleroexec.RunCommand(cmd.Cmd())
	rm.log.WithFields(logrus.Fields{
		"repository": cmd.RepoName(),
		"command":    cmd.String(),
		"stdout":     stdout,
		"stderr":     stderr,
	}).Debugf("Ran restic command")
	if err != nil {
		return errors.Wrapf(err, "error running command=%s, stdout=%s, stderr=%s", cmd.String(), stdout, stderr)
	}

	return nil
}

// GetInsecureSkipTLSVerifyFromBSL get insecureSkipTLSVerify flag from BSL configuration,
// Then return --insecure-tls flag with boolean value as result.
func GetInsecureSkipTLSVerifyFromBSL(backupLocation *velerov1api.BackupStorageLocation, logger logrus.FieldLogger) string {
	result := ""

	if backupLocation == nil {
		logger.Info("bsl is nil. return empty.")
		return result
	}

	if insecure, _ := strconv.ParseBool(backupLocation.Spec.Config[insecureSkipTLSVerifyKey]); insecure {
		logger.Debugf("set --insecure-tls=true for Restic command according to BSL %s config", backupLocation.Name)
		result = resticInsecureTLSFlag + "=true"
		return result
	}

	return result
}
