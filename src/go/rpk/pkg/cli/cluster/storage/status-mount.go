// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package storage

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	dataplanev1alpha2 "buf.build/gen/go/redpandadata/dataplane/protocolbuffers/go/redpanda/api/dataplane/v1alpha2"
	"connectrpc.com/connect"
	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newMountStatus(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "status-mount [MIGRATION ID]",
		Short:   "Status of mount/unmount operation",
		Long:    "Status of mount/unmount operation on topic to Redpanda cluster from Tiered Storage",
		Aliases: []string{"status-unmount"},
		Example: `
Status for a mount/unmount operation
	rpk cluster storage status-mount 123
`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, from []string) {
			f := p.Formatter
			if h, ok := f.Help(migrationState{}); ok {
				out.Exit(h)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitServerlessAdmin(p)

			migrationID, err := strconv.Atoi(from[0])
			out.MaybeDie(err, "invalid migration ID: %v", err)

			var mState rpadmin.MigrationState
			if p.FromCloud {
				cl, err := createDataplaneClient(p)
				out.MaybeDieErr(err)

				resp, err := cl.CloudStorage.GetMountTask(
					cmd.Context(),
					connect.NewRequest(
						&dataplanev1alpha2.GetMountTaskRequest{
							Id: int32(migrationID),
						},
					),
				)
				out.MaybeDie(err, "unable to get the status of mount/unmount operation: %v", err)
				if resp != nil {
					mState = mountTaskToAdminMigrationState(resp.Msg)
				}
			} else {
				adm, err := adminapi.NewClient(cmd.Context(), fs, p)
				out.MaybeDie(err, "unable to initialize admin client: %v", err)

				mState, err = adm.GetMigration(cmd.Context(), migrationID)
				out.MaybeDie(err, "unable to get the status of the migration: %v", err)
			}
			outStatus := migrationState{
				ID:            mState.ID,
				State:         mState.State,
				MigrationType: mState.Migration.MigrationType,
				Topics:        rpadminTopicsToStringSlice(mState.Migration.Topics),
			}
			printDetailedStatusMount(f, outStatus, os.Stdout)
		},
	}
	p.InstallFormatFlag(cmd)
	return cmd
}

func printDetailedStatusMount(f config.OutFormatter, d migrationState, w io.Writer) {
	if isText, _, t, err := f.Format(d); !isText {
		out.MaybeDie(err, "unable to print in the requested format %q: %v", f.Kind, err)
		fmt.Fprintln(w, t)
		return
	}
	tw := out.NewTableTo(w, "ID", "State", "Migration", "Topics")
	defer tw.Flush()
	tw.Print(d.ID, d.State, d.MigrationType, strings.Join(d.Topics, ", "))
}

func mountTaskToAdminMigrationState(resp *dataplanev1alpha2.GetMountTaskResponse) rpadmin.MigrationState {
	var state rpadmin.MigrationState
	if resp != nil {
		task := resp.Task
		state = rpadmin.MigrationState{
			ID:    int(task.Id),
			State: strings.TrimPrefix(task.State.String(), "STATE_"),
			Migration: rpadmin.Migration{
				MigrationType: task.Type.String(),
				Topics:        mountTaskTopicsToNamespacedOrInboundTopics(task.Topics, task.Type),
			},
		}
	}
	return state
}