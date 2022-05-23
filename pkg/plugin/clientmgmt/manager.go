/*
Copyright 2020 the Velero contributors.

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

package clientmgmt

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"

	biav1cli "github.com/vmware-tanzu/velero/pkg/plugin/clientmgmt/backupitemaction/v1"
	biav2cli "github.com/vmware-tanzu/velero/pkg/plugin/clientmgmt/backupitemaction/v2"
	ibav1cli "github.com/vmware-tanzu/velero/pkg/plugin/clientmgmt/itemblockaction/v1"
	"github.com/vmware-tanzu/velero/pkg/plugin/clientmgmt/process"
	riav1cli "github.com/vmware-tanzu/velero/pkg/plugin/clientmgmt/restoreitemaction/v1"
	riav2cli "github.com/vmware-tanzu/velero/pkg/plugin/clientmgmt/restoreitemaction/v2"
	vsv1cli "github.com/vmware-tanzu/velero/pkg/plugin/clientmgmt/volumesnapshotter/v1"
	"github.com/vmware-tanzu/velero/pkg/plugin/framework/common"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	biav1 "github.com/vmware-tanzu/velero/pkg/plugin/velero/backupitemaction/v1"
	biav2 "github.com/vmware-tanzu/velero/pkg/plugin/velero/backupitemaction/v2"
	ibav1 "github.com/vmware-tanzu/velero/pkg/plugin/velero/itemblockaction/v1"
	riav1 "github.com/vmware-tanzu/velero/pkg/plugin/velero/restoreitemaction/v1"
	riav2 "github.com/vmware-tanzu/velero/pkg/plugin/velero/restoreitemaction/v2"
	vsv1 "github.com/vmware-tanzu/velero/pkg/plugin/velero/volumesnapshotter/v1"
)

// zhou: implemented by "manager struct"

// Manager manages the lifecycles of plugins.
type Manager interface {
	// zhou: get "PluginKindObjectStore" plugins' handler.
	//       Only allow one BSL for a Backup CR.

	// GetObjectStore returns the ObjectStore plugin for name.
	GetObjectStore(name string) (velero.ObjectStore, error)

	// zhou: get "PluginKindVolumeSnapshotter" plugins' handler.
	//       Only allow one VSL for a Backup CR.

	// GetVolumeSnapshotter returns the VolumeSnapshotter plugin for name.
	GetVolumeSnapshotter(name string) (vsv1.VolumeSnapshotter, error)

	// zhou: get "PluginKindBackupItemAction" plugins' handler.

	// GetBackupItemActions returns all v1 backup item action plugins.
	GetBackupItemActions() ([]biav1.BackupItemAction, error)

	// GetBackupItemAction returns the backup item action plugin for name.
	GetBackupItemAction(name string) (biav1.BackupItemAction, error)

	// GetBackupItemActionsV2 returns all v2 backup item action plugins (including those adapted from v1).
	GetBackupItemActionsV2() ([]biav2.BackupItemAction, error)

	// GetBackupItemActionV2 returns the backup item action plugin for name.
	GetBackupItemActionV2(name string) (biav2.BackupItemAction, error)

	// zhou: get "PluginKindRestoreItemAction" plugins' handler.

	// GetRestoreItemActions returns all restore item action plugins.
	GetRestoreItemActions() ([]riav1.RestoreItemAction, error)

	// GetRestoreItemAction returns the restore item action plugin for name.
	GetRestoreItemAction(name string) (riav1.RestoreItemAction, error)

	// GetRestoreItemActionsV2 returns all v2 restore item action plugins.
	GetRestoreItemActionsV2() ([]riav2.RestoreItemAction, error)

	// GetRestoreItemActionV2 returns the restore item action plugin for name.
	GetRestoreItemActionV2(name string) (riav2.RestoreItemAction, error)

	// zhou: get "PluginKindDeleteItemAction" plugins' handler.

	// GetDeleteItemActions returns all delete item action plugins.
	GetDeleteItemActions() ([]velero.DeleteItemAction, error)

	// GetDeleteItemAction returns the delete item action plugin for name.
	GetDeleteItemAction(name string) (velero.DeleteItemAction, error)

	// GetItemBlockActions returns all v1 ItemBlock action plugins.
	GetItemBlockActions() ([]ibav1.ItemBlockAction, error)

	// GetItemBlockAction returns the ItemBlock action plugin for name.
	GetItemBlockAction(name string) (ibav1.ItemBlockAction, error)

	// CleanupClients terminates all of the Manager's running plugin processes.
	CleanupClients()
}

// Used checking for adapted plugin versions
var pluginNotFoundErrType = &process.PluginNotFoundError{}

// manager implements Manager.
type manager struct {
	logger   logrus.FieldLogger
	logLevel logrus.Level

	// zhou: "clientmgmt.registry"

	registry process.Registry

	restartableProcessFactory process.RestartableProcessFactory

	// lock guards restartableProcesses
	lock sync.Mutex

	// zhou: all running Plugin Binary process within a PluginManager,
	//       "Plugin bianary name" -> "RestartableProcess"

	restartableProcesses map[string]process.RestartableProcess
}

// zhou: create plugin clients manager instance.

// NewManager constructs a manager for getting plugins.
func NewManager(logger logrus.FieldLogger, level logrus.Level, registry process.Registry) Manager {
	return &manager{
		logger:   logger,
		logLevel: level,
		registry: registry,

		restartableProcessFactory: process.NewRestartableProcessFactory(),

		restartableProcesses: make(map[string]process.RestartableProcess),
	}
}

// zhou: kill all plugin processes managed by this plugin manager.
func (m *manager) CleanupClients() {
	m.lock.Lock()

	for _, restartableProcess := range m.restartableProcesses {
		restartableProcess.Stop()
	}

	m.lock.Unlock()
}

// zhou: get plugin's "RestartableProcess" by PluginKind and Registered plugin name,
//       start it if not running.
//       The process's lifecycle is managed by plugin manager, and plugin manager is always created
//       by each Reconcile().

// getRestartableProcess returns a restartableProcess for a plugin identified by kind and name, creating a
// restartableProcess if it is the first time it has been requested.
func (m *manager) getRestartableProcess(kind common.PluginKind, name string) (process.RestartableProcess, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	logger := m.logger.WithFields(logrus.Fields{
		"kind": kind.String(),
		"name": name,
	})
	logger.Debug("looking for plugin in registry")

	// zhou: get 'PluginIdentifier" by PluginKind and registered plugin name.
	info, err := m.registry.Get(kind, name)
	if err != nil {
		return nil, err
	}

	logger = logger.WithField("command", info.Command)

	// zhou: check whether plugin process created before.
	restartableProcess, found := m.restartableProcesses[info.Command]
	if found {
		logger.Debug("found preexisting restartable plugin process")
		return restartableProcess, nil
	}

	logger.Debug("creating new restartable plugin process")

	// zhou: if there is no corresponding process for this plugin, create it.

	restartableProcess, err = m.restartableProcessFactory.NewRestartableProcess(info.Command, m.logger, m.logLevel)
	if err != nil {
		return nil, err
	}

	m.restartableProcesses[info.Command] = restartableProcess

	return restartableProcess, nil
}

// zhou: used by "objectBackupStoreGetter.Get()", e.g. "name" == "aws"
//       only allow one BSL for one Backup CR.

// GetObjectStore returns a restartableObjectStore for name.
func (m *manager) GetObjectStore(name string) (velero.ObjectStore, error) {
	// zhou: transform to "velero.io/aws", which is same as velero-plugin-for-aws implemented:
	//       'RegisterObjectStore("velero.io/aws", newAwsObjectStore) '
	name = sanitizeName(name)

	restartableProcess, err := m.getRestartableProcess(common.PluginKindObjectStore, name)
	if err != nil {
		return nil, err
	}

	r := NewRestartableObjectStore(name, restartableProcess)

	return r, nil
}

// zhou: "name" is VSL. Only allow one VSL for one Backup CR.

// GetVolumeSnapshotter returns a restartableVolumeSnapshotter for name.
func (m *manager) GetVolumeSnapshotter(name string) (vsv1.VolumeSnapshotter, error) {
	name = sanitizeName(name)

	for _, adaptedVolumeSnapshotter := range vsv1cli.AdaptedVolumeSnapshotters() {
		restartableProcess, err := m.getRestartableProcess(adaptedVolumeSnapshotter.Kind, name)
		// Check if plugin was not found
		if errors.As(err, &pluginNotFoundErrType) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return adaptedVolumeSnapshotter.GetRestartable(name, restartableProcess), nil
	}
	return nil, fmt.Errorf("unable to get valid VolumeSnapshotter for %q", name)
}

// zhou: get all registered "BackItemAction" plugins' rpc call instances.

// GetBackupItemActions returns all backup item actions as restartableBackupItemActions.
func (m *manager) GetBackupItemActions() ([]biav1.BackupItemAction, error) {
	list := m.registry.List(common.PluginKindBackupItemAction)

	actions := make([]biav1.BackupItemAction, 0, len(list))

	// zhou: why not "for _, id := range list" ???
	for i := range list {
		id := list[i]

		r, err := m.GetBackupItemAction(id.Name)
		if err != nil {
			return nil, err
		}

		actions = append(actions, r)
	}

	return actions, nil
}

// zhou: get rpc call instance, by "registered plugin name" like "velero.io/csi-pvc-backupper"

// GetBackupItemAction returns a restartableBackupItemAction for name.
func (m *manager) GetBackupItemAction(name string) (biav1.BackupItemAction, error) {
	name = sanitizeName(name)

	for _, adaptedBackupItemAction := range biav1cli.AdaptedBackupItemActions() {
		// zhou: process used to talk
		restartableProcess, err := m.getRestartableProcess(adaptedBackupItemAction.Kind, name)
		// Check if plugin was not found
		if errors.As(err, &pluginNotFoundErrType) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return adaptedBackupItemAction.GetRestartable(name, restartableProcess), nil
	}
	return nil, fmt.Errorf("unable to get valid BackupItemAction for %q", name)
}

// GetBackupItemActionsV2 returns all v2 backup item actions as RestartableBackupItemActions.
func (m *manager) GetBackupItemActionsV2() ([]biav2.BackupItemAction, error) {
	list := m.registry.List(common.PluginKindBackupItemActionV2)

	actions := make([]biav2.BackupItemAction, 0, len(list))

	for i := range list {
		id := list[i]

		r, err := m.GetBackupItemActionV2(id.Name)
		if err != nil {
			return nil, err
		}

		actions = append(actions, r)
	}

	return actions, nil
}

// GetBackupItemActionV2 returns a v2 restartableBackupItemAction for name.
func (m *manager) GetBackupItemActionV2(name string) (biav2.BackupItemAction, error) {
	name = sanitizeName(name)

	for _, adaptedBackupItemAction := range biav2cli.AdaptedBackupItemActions() {
		restartableProcess, err := m.getRestartableProcess(adaptedBackupItemAction.Kind, name)
		// Check if plugin was not found
		if errors.As(err, &pluginNotFoundErrType) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return adaptedBackupItemAction.GetRestartable(name, restartableProcess), nil
	}
	return nil, fmt.Errorf("unable to get valid BackupItemActionV2 for %q", name)
}

// zhou: get all handlers of plugins which "RegisterRestoreItemAction()"

// GetRestoreItemActions returns all restore item actions as restartableRestoreItemActions.
func (m *manager) GetRestoreItemActions() ([]riav1.RestoreItemAction, error) {
	list := m.registry.List(common.PluginKindRestoreItemAction)

	actions := make([]riav1.RestoreItemAction, 0, len(list))

	for i := range list {
		id := list[i]

		r, err := m.GetRestoreItemAction(id.Name)
		if err != nil {
			return nil, err
		}

		actions = append(actions, r)
	}

	return actions, nil
}

// GetRestoreItemAction returns a restartableRestoreItemAction for name.
func (m *manager) GetRestoreItemAction(name string) (riav1.RestoreItemAction, error) {
	name = sanitizeName(name)

	for _, adaptedRestoreItemAction := range riav1cli.AdaptedRestoreItemActions() {
		restartableProcess, err := m.getRestartableProcess(adaptedRestoreItemAction.Kind, name)
		// Check if plugin was not found
		if errors.As(err, &pluginNotFoundErrType) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return adaptedRestoreItemAction.GetRestartable(name, restartableProcess), nil
	}
	return nil, fmt.Errorf("unable to get valid RestoreItemAction for %q", name)
}

// GetRestoreItemActionsV2 returns all v2 restore item actions as restartableRestoreItemActions.
func (m *manager) GetRestoreItemActionsV2() ([]riav2.RestoreItemAction, error) {
	list := m.registry.List(common.PluginKindRestoreItemActionV2)

	actions := make([]riav2.RestoreItemAction, 0, len(list))

	for i := range list {
		id := list[i]

		r, err := m.GetRestoreItemActionV2(id.Name)
		if err != nil {
			return nil, err
		}

		actions = append(actions, r)
	}

	return actions, nil
}

// GetRestoreItemActionV2 returns a v2 restartableRestoreItemAction for name.
func (m *manager) GetRestoreItemActionV2(name string) (riav2.RestoreItemAction, error) {
	name = sanitizeName(name)

	for _, adaptedRestoreItemAction := range riav2cli.AdaptedRestoreItemActions() {
		restartableProcess, err := m.getRestartableProcess(adaptedRestoreItemAction.Kind, name)
		// Check if plugin was not found
		if errors.As(err, &pluginNotFoundErrType) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return adaptedRestoreItemAction.GetRestartable(name, restartableProcess), nil
	}
	return nil, fmt.Errorf("unable to get valid RestoreItemActionV2 for %q", name)
}

// zhou: get all handlers of plugins which "RegisterDeleteItemAction()"

// GetDeleteItemActions returns all delete item actions as restartableDeleteItemActions.
func (m *manager) GetDeleteItemActions() ([]velero.DeleteItemAction, error) {
	list := m.registry.List(common.PluginKindDeleteItemAction)

	actions := make([]velero.DeleteItemAction, 0, len(list))

	for i := range list {
		id := list[i]

		r, err := m.GetDeleteItemAction(id.Name)
		if err != nil {
			return nil, err
		}

		actions = append(actions, r)
	}

	return actions, nil
}

// GetDeleteItemAction returns a restartableDeleteItemAction for name.
func (m *manager) GetDeleteItemAction(name string) (velero.DeleteItemAction, error) {
	name = sanitizeName(name)

	restartableProcess, err := m.getRestartableProcess(common.PluginKindDeleteItemAction, name)
	if err != nil {
		return nil, err
	}

	r := NewRestartableDeleteItemAction(name, restartableProcess)
	return r, nil
}

// GetItemBlockActions returns all ItemBlock actions as restartableItemBlockActions.
func (m *manager) GetItemBlockActions() ([]ibav1.ItemBlockAction, error) {
	list := m.registry.List(common.PluginKindItemBlockAction)

	actions := make([]ibav1.ItemBlockAction, 0, len(list))

	for i := range list {
		id := list[i]

		r, err := m.GetItemBlockAction(id.Name)
		if err != nil {
			return nil, err
		}

		actions = append(actions, r)
	}

	return actions, nil
}

// GetItemBlockAction returns a restartableItemBlockAction for name.
func (m *manager) GetItemBlockAction(name string) (ibav1.ItemBlockAction, error) {
	name = sanitizeName(name)

	for _, adaptedItemBlockAction := range ibav1cli.AdaptedItemBlockActions() {
		restartableProcess, err := m.getRestartableProcess(adaptedItemBlockAction.Kind, name)
		// Check if plugin was not found
		if errors.As(err, &pluginNotFoundErrType) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return adaptedItemBlockAction.GetRestartable(name, restartableProcess), nil
	}
	return nil, fmt.Errorf("unable to get valid ItemBlockAction for %q", name)
}

// sanitizeName adds "velero.io" to legacy plugins that weren't namespaced.
func sanitizeName(name string) string {
	// Backwards compatibility with non-namespaced Velero plugins, following principle of least surprise
	// since DeleteItemActions were not bundled with Velero when plugins were non-namespaced.
	if !strings.Contains(name, "/") {
		name = "velero.io/" + name
	}

	return name
}
