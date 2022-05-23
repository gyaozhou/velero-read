/*
Copyright 2019 the Velero contributors.

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

package framework

import (
	plugin "github.com/hashicorp/go-plugin"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/vmware-tanzu/velero/pkg/plugin/framework/common"
	protobiav1 "github.com/vmware-tanzu/velero/pkg/plugin/generated"
)

// zhou: implemented "github.com/hashicorp/go-plugin, type GRPCPlugin interface {}"

// BackupItemActionPlugin is an implementation of go-plugin's Plugin
// interface with support for gRPC for the backup/ItemAction
// interface.
type BackupItemActionPlugin struct {
	// zhou: disable net/rpc, using "type Plugin interface {}"
	//       The following "GRPCClient" and "GRPCServer" implement "type GRPCPlugin interface {}"
	plugin.NetRPCUnsupportedPlugin

	// zhou: defined in pkg/plugin/framework/plugin_base.go
	*common.PluginBase
}

// GRPCClient returns a clientDispenser for BackupItemAction gRPC clients.
func (p *BackupItemActionPlugin) GRPCClient(_ context.Context, _ *plugin.GRPCBroker, clientConn *grpc.ClientConn) (any, error) {
	// zhou: create a client dispenser for each kind Plugin.
	return common.NewClientDispenser(p.ClientLogger, clientConn, newBackupItemActionGRPCClient), nil
}

// GRPCServer registers a BackupItemAction gRPC server.
func (p *BackupItemActionPlugin) GRPCServer(_ *plugin.GRPCBroker, server *grpc.Server) error {
	protobiav1.RegisterBackupItemActionServer(server, &BackupItemActionGRPCServer{mux: p.ServerMux})
	return nil
}
