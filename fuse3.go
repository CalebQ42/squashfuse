package squashfuse

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/CalebQ42/fuse"
	"github.com/CalebQ42/fuse/fs"
	"github.com/CalebQ42/squashfs"
	squashfslow "github.com/CalebQ42/squashfs/low"
	"github.com/CalebQ42/squashfs/low/inode"
)

type Mount struct {
	r         *squashfslow.Reader
	con       *fuse.Conn
	mountDone chan struct{}
}

func NewMount(r *squashfs.Reader) *Mount {
	return NewMountFromLow(r.Low)
}

func NewMountFromLow(r *squashfslow.Reader) *Mount {
	return &Mount{r: r}
}

// Mounts the archive to the given mountpoint using fuse3. Non-blocking.
// If Unmount does not get called, the mount point must be unmounted using umount before the directory can be used again.
func (m *Mount) Mount(mountpoint string) (err error) {
	if m.con != nil {
		return errors.New("squashfs archive already mounted")
	}
	m.con, err = fuse.Mount(mountpoint, fuse.ReadOnly())
	if err != nil {
		return
	}
	<-m.con.Ready
	m.mountDone = make(chan struct{})
	go func() {
		fs.Serve(m.con, squashFuse{r: m.r})
		close(m.mountDone)
	}()
	return
}

// Blocks until the mount ends.
func (m *Mount) MountWait() {
	if m.mountDone != nil {
		<-m.mountDone
	}
}

// Unmounts the archive.
func (m *Mount) Unmount() error {
	if m.con != nil {
		defer func() { m.con = nil }()
		return m.con.Close()
	}
	return errors.New("squashfs archive is not mounted")
}

type squashFuse struct {
	r *squashfslow.Reader
}

func (s squashFuse) Root() (fs.Node, error) {
	return fileNode{
		FileBase: &s.r.Root.FileBase,
		r:        s.r,
	}, nil
}

type fileNode struct {
	*squashfslow.FileBase
	r *squashfslow.Reader
}

func (f fileNode) Attr(ctx context.Context, attr *fuse.Attr) error {
	var err error
	attr.Gid, err = f.r.Id(f.Inode.GidInd)
	if err != nil {
		return err
	}
	attr.Uid, err = f.r.Id(f.Inode.UidInd)
	if err != nil {
		return err
	}
	attr.Size = f.Inode.Size()
	attr.Blocks = f.Inode.Size() / 512
	if f.Inode.Size()%512 > 0 {
		attr.Blocks++
	}
	attr.Inode = uint64(f.Inode.Num)
	attr.Mode = f.Inode.Mode()
	attr.Nlink = f.Inode.LinkCount()
	return nil
}

func (f fileNode) Id() uint64 {
	return uint64(f.Inode.Num)
}

func (f fileNode) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	switch f.Inode.Type {
	case inode.Sym:
		return string(f.Inode.Data.(inode.Symlink).Target), nil
	case inode.ESym:
		return string(f.Inode.Data.(inode.ESymlink).Target), nil
	}
	return "", nil
}

func (f fileNode) Lookup(ctx context.Context, name string) (fs.Node, error) {
	asFS, err := f.ToDir(f.r)
	if err != nil {
		return nil, fuse.ENOTDIR
	}
	ret, err := asFS.Open(f.r, name)
	if err != nil {
		return nil, fuse.ENOENT
	}
	return fileNode{FileBase: ret}, nil
}

func (f fileNode) ReadAll(ctx context.Context) ([]byte, error) {
	if f.IsRegular() {
		rdr, err := f.GetFullReader(f.r)
		if err != nil {
			return nil, err
		}
		var buf bytes.Buffer
		_, err = rdr.WriteTo(&buf)
		return buf.Bytes(), err
	}
	return nil, ENODATA
}

func (f fileNode) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	if f.IsRegular() {
		rdr, err := f.GetReader(f.r)
		if err != nil {
			return err
		}
		buf := make([]byte, req.Size)
		rdr.Read(make([]byte, req.Offset))
		n, err := rdr.Read(buf)
		if err == io.EOF {
			resp.Data = buf[:n]
		}
		return nil
	}
	return ENODATA
}

func (f fileNode) ReadDirAll(ctx context.Context) (out []fuse.Dirent, err error) {
	asFS, err := f.ToDir(f.r)
	if err != nil {
		return nil, fuse.ENOTDIR
	}
	var t fuse.DirentType
	for i := range asFS.Entries {
		switch asFS.Entries[i].InodeType {
		case inode.Fil:
			t = fuse.DT_File
		case inode.Dir:
			t = fuse.DT_Dir
		case inode.Block:
			t = fuse.DT_Block
		case inode.Sym:
			t = fuse.DT_Link
		case inode.Char:
			t = fuse.DT_Char
		case inode.Fifo:
			t = fuse.DT_FIFO
		case inode.Sock:
			t = fuse.DT_Socket
		default:
			t = fuse.DT_Unknown
		}
		out = append(out, fuse.Dirent{
			Inode: uint64(asFS.Entries[i].Num),
			Type:  t,
			Name:  asFS.Entries[i].Name,
		})
	}
	return
}
