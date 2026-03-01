/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package pb

import (
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

func Exec(argv ...string) error {
	cmd := exec.Command(argv[0], argv[1:]...)

	if err := cmd.Start(); err != nil {
		return err
	}
	return cmd.Wait()
}

func TestProtosRegenerate(t *testing.T) {
	err := Exec("./gen.sh")
	require.NoError(t, err, "Got error while regenerating protos: %v\n", err)

	generatedProtos := []string{"badgerpb4.pb.go", "badgerpb4_vtproto.pb.go", "wal_replication.pb.go", "wal_replication_vtproto.pb.go"}
	args := append([]string{"git", "diff", "--quiet", "--"}, generatedProtos...)
	err = Exec(args...)
	require.NoError(t, err, "protobuf files changed after regenerating")
}
