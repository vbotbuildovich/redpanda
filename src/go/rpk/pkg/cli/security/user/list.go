// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package user

import (
	dataplanev1alpha2 "buf.build/gen/go/redpandadata/dataplane/protocolbuffers/go/redpanda/api/dataplane/v1alpha2"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newListUsersCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List SASL users",
		Args:    cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			f := p.Formatter
			if h, ok := f.Help([]string{}); ok {
				out.Exit(h)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			var users []string
			if p.FromCloud {
				cl, err := p.DataplaneClient()
				out.MaybeDie(err, "unable to initialize cloud client: %v", err)

				listUsers, err := cl.User.ListUsers(cmd.Context(), connect.NewRequest(&dataplanev1alpha2.ListUsersRequest{}))
				out.MaybeDie(err, "unable to list users: %v", err)
				if listUsers != nil {
					users = dataplaneListUserToString(listUsers.Msg)
				}
			} else {
				cl, err := adminapi.NewClient(cmd.Context(), fs, p)
				out.MaybeDie(err, "unable to initialize admin client: %v", err)

				users, err = cl.ListUsers(cmd.Context())
				out.MaybeDie(err, "unable to list users: %v", err)
			}
			if isText, _, s, err := f.Format(users); !isText {
				out.MaybeDie(err, "unable to print in the required format %q: %v", f.Kind, err)
				out.Exit(s)
			}
			tw := out.NewTable("Username")
			defer tw.Flush()
			for _, u := range users {
				tw.Print(u)
			}
		},
	}
}

func dataplaneListUserToString(resp *dataplanev1alpha2.ListUsersResponse) []string {
	var users []string
	if resp != nil {
		for _, u := range resp.Users {
			users = append(users, u.Name)
		}
	}
	return users
}
