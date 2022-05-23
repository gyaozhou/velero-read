/*
Copyright 2017 the Velero contributors.

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

package backup

import (
	"github.com/spf13/cobra"

	"github.com/vmware-tanzu/velero/pkg/client"
)

// zhou: velero backup create/get/describe/delete
func NewCommand(f client.Factory) *cobra.Command {
	c := &cobra.Command{
		// zhou: this field specify the command format.
		Use:   "backup",
		Short: "Work with backups",
		Long:  "Work with backups",
	}

	// zhou: add subcommands
	c.AddCommand(
		// zhou: shared subcommand,
		//       "velero backup create ..." and "velero create backup ..."
		NewCreateCommand(f, "create"),
		// zhou: shared subcommand,
		//       "velero backup get ..." and "velero get backup ..."
		NewGetCommand(f, "get"),
		// zhou: only be used as "velero backup log
		NewLogsCommand(f),
		NewDescribeCommand(f, "describe"),
		NewDownloadCommand(f),
		NewDeleteCommand(f, "delete"),
	)

	return c
}
