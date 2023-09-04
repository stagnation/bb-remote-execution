package runner

import (
	"context"
	"os"
	"strings"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_runner"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type mountingRunner struct {
	base        runner_pb.RunnerServer
	buildDirectory filesystem.Directory
	mount *bb_runner.MountInfo
}

// NewMountingRunner is a decorator for Runner
// that mounts `mount` before running a build action.
//
// This decorator can be used for chroot runners
// that must mount special filesystems into the input root.
func NewMountingRunner(base runner_pb.RunnerServer, buildDirectory filesystem.Directory, mount *bb_runner.MountInfo) runner_pb.RunnerServer {
	return &mountingRunner{
		buildDirectory: buildDirectory,
		mount: mount,
		base: base,
	}
}

func Open(buildDirectory filesystem.Directory, inputRootDirectory string) (filesystem.DirectoryCloser, error) {
	// TODO(nils): How to best open the input root `Directory`
	//             It must be a `localDirectory`, but outer directories can be `lazyDirectory`.
	// NB: We iterate over Directory, as the initial buildDirectory is not a DirectoryCloser.
	iterator := buildDirectory
	var closer filesystem.DirectoryCloser

	// TODO: if parts is empty, or ".", make sure to enter it and create a Closer.
	parts := strings.Split(inputRootDirectory, "/")
	for i, segment := range parts {
		var err error
		component, ok := path.NewComponent(segment)
		if ! ok {
			return nil, status.Errorf(codes.FailedPrecondition, "Could not construct component: %#v", segment)
		}
		closer, err = iterator.EnterDirectory(component)
		if err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "Could not enter directory component %#v in %#v", segment, buildDirectory)
		}
		if i < len(parts) - 1 {
			defer closer.Close()
		}

		iterator = closer.(filesystem.Directory)
	}

	return closer, nil
}

func (r *mountingRunner) Run(ctx context.Context, request *runner_pb.RunRequest) (response *runner_pb.RunResponse, err error) {
	root, err := Open(r.buildDirectory, request.InputRootDirectory)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "Could not enter input root directory %#v", request.InputRootDirectory)
	}
	defer root.Close()

	mountpoint := path.MustNewComponent(r.mount.Mountpoint)
	err = root.Mkdir(mountpoint, 0o555)
	if err != nil {
		if ! os.IsExist(err) {
			return nil, util.StatusWrapf(err, "Failed to create mount point: %#v in the input root", r.mount.Mountpoint)
		}
	}

	if err := root.Mount(mountpoint, r.mount.Source, r.mount.FilesystemType); err != nil {
		return nil, util.StatusWrapf(err, "Failed to mount %#v in the input root", r.mount)
	}

	response, err = r.base.Run(ctx, request)
	if err2 := root.Unmount(mountpoint); err2 != nil {
		if err == nil {
			err = err2
		} else {
			err = util.StatusFromMultiple([]error{
				err,
				util.StatusWrap(err2, "Failed to unmount %#v in the input root"),
			})
		}
	}
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (r *mountingRunner) CheckReadiness(ctx context.Context, request *emptypb.Empty) (*emptypb.Empty, error) {
	return r.base.CheckReadiness(ctx, request)
}
