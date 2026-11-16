package client

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"wosh/internal/build"
	"wosh/internal/stream"
	"wosh/internal/wire"
)

// Remote start: use an installed wosh, upgrade an older installed one, or
// run this build from memory.
//
// The remote command is a perl one-liner (perl ships with practically every
// Linux distribution and with macOS). It reports the platform, receives a
// compressed wosh build for it over the same ssh stdin and executes it. The
// build is xz-compressed when the remote has an `xz` command (and we have
// the .xz cached), else gzipped (perl's IO::Uncompress::Gunzip), else raw:
//
//   - Linux: from a memfd (memfd_create), so it never touches a filesystem.
//   - otherwise / memfd blocked: written to /dev/shm, $TMPDIR, /tmp or $HOME
//     (first that works), executed, and deleted by wosh as soon as its
//     session daemon is running.
//
// Reconnects do not need the binary again: pushAttachStub is a tiny perl
// relay between the ssh pipe and the daemon's control socket.
//
// Both stubs are embedded in single quotes on the remote command line and
// must stay free of single quotes, "\\" and "!" so bash, zsh, fish, tcsh and
// sh all pass them through unchanged.

// pushStub: reports the platform and every installed wosh it can find
// ($PATH, ~/bin, ~/.local/bin, ~/go/bin) as "<path>|<writable>|<_proto
// output>", then follows the client's decision (see decide):
//
//	USE <path>                 exec an installed wosh
//	UPGRADE <path> <len> <c>   replace it atomically with the payload, exec it
//	MEM <len> <c>              run the payload without installing it
//
// The capability field is "1" if perl can gunzip ("0" if not), with "x"
// appended if `xz` is on PATH; <c> is the payload's codec: x (xz, piped
// through `xz -dc`), g (gzip) or r (raw).
const pushStub = `$|=1;$o=$^O;$m=` + "`uname -m`" + `;chomp $m;` +
	`$z=eval{require IO::Uncompress::Gunzip;1}?1:0;@f=();%s=();$X="";` +
	`for $c (split /:/,$ENV{PATH}){$X||="$c/xz" if -f "$c/xz" and -x "$c/xz"}$z.="x" if $X;` +
	`for $c ((split /:/,$ENV{PATH}),"$ENV{HOME}/bin","$ENV{HOME}/.local/bin","$ENV{HOME}/go/bin"){` +
	`$x="$c/wosh";next if $s{$x}++ or $x=~/[|\s]/;next unless -f $x and -x $x;` +
	`$pv=` + "`\"$x\" _proto 2>/dev/null`" + `;chomp $pv;$pv=~s/[|\s]//g;$pv="-" unless length $pv;` +
	`$w=(-w $x and -w $c)?1:0;push @f,"$x|$w|$pv"}` +
	`print "WOSHPUSH $o $m $z @f\n";binmode STDIN;$l=<STDIN>;defined $l or exit 1;chomp $l;` +
	`($q,$a,$l,$c)=split / /,$l;if($q eq "USE"){exec {$a} "wosh",@ARGV;die "wosh: exec $a failed\n"}` +
	`($l,$c)=($a,$l) if $q eq "MEM";` +
	`$d="";while(length($d)<$l){$r=read(STDIN,$d,$l-length($d),length($d));$r or die "wosh push: short read\n"}` +
	`if($c eq "x"){$p=open($h,"-|");defined $p or die "wosh push: fork: $^E\n";` +
	`unless($p){open($k,"|-",$X,"-dc") or exit 1;binmode $k;print $k $d;close $k or exit 1;exit 0}` +
	`binmode $h;{local $/;$d=<$h>}close $h or die "wosh push: xz failed\n"}` +
	`elsif($c eq "g"){$u="";IO::Uncompress::Gunzip::gunzip(\$d=>\$u) or die "wosh push: gunzip failed\n";$d=$u}` +
	`if($q eq "UPGRADE"){$t="$a.new.$$";if(open($h,">",$t)){binmode $h;print $h $d;close $h;chmod 0755,$t;` +
	`if(rename $t,$a){print STDERR "wosh: upgraded $a\n";exec {$a} "wosh",@ARGV}unlink $t}}` +
	`%n=(x86_64=>319,aarch64=>279);$nm="wosh";` +
	`if($o eq "linux" and $n{$m}){$f=syscall($n{$m},$nm,0);if($f>=0){open($h,">&=",$f) or die;binmode $h;$w=0;` +
	`while($w<length $d){$k=syswrite($h,$d,length($d)-$w,$w);defined $k or die "wosh push: write: $^E\n";$w+=$k}` +
	`exec {"/proc/self/fd/$f"} "wosh",@ARGV}}` +
	`for $t ("/dev/shm",$ENV{TMPDIR}||"/tmp","/tmp",$ENV{HOME}){next unless $t and -d $t;$p="$t/.wosh-$$";` +
	`open($h,">",$p) or next;binmode $h;print $h $d;close $h;chmod 0700,$p;$ENV{WOSH_UNLINK}=$p;` +
	`exec {$p} "wosh",@ARGV;unlink $p}die "wosh push: cannot execute pushed binary\n"`

const pushAttachStub = `use IO::Socket::UNIX;use MIME::Base64;$|=1;($k,$i)=@ARGV;` +
	`sub osc{print "\e]5379;wosh;".encode_base64($_[0],"")."\a"}` +
	`$s=IO::Socket::UNIX->new(Peer=>$k);unless($s){osc(qq({"t":"err","id":"$i","err":"no such session"}));exit 1}` +
	`osc(qq({"t":"hello","id":"$i","v":1}));$/="\r";` +
	`while(<STDIN>){if(/WOSH:([A-Za-z0-9+\/=]+)/){print $s decode_base64($1)."\n";` +
	`{local $/="\n";$r=<$s>}defined $r or exit 1;chomp $r;osc($r);exit 0 if $r=~/"t":"answer"/}}`

// hopStub runs ssh on an intermediate host with that host's own config and
// keys: argv (NUL-separated, base64) is passed to ssh unchanged, so nested
// remote commands need no quoting through the intermediate login shell.
// WOSH_HOP_SSH overrides the ssh program (tests).
const hopStub = `use MIME::Base64;exec {$ENV{WOSH_HOP_SSH}||"ssh"} "ssh",split(chr(0),decode_base64($ARGV[0]));` +
	`die "wosh: cannot run ssh: $^E\n"`

// hopCommand builds a remote command that ssh's through hops (each the
// ssh args used on the previous host) and finally runs final.
func hopCommand(hops [][]string, final string) string {
	if len(hops) == 0 {
		return final
	}
	argv := append([]string{"-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10"}, hops[0]...)
	argv = append(argv, hopCommand(hops[1:], final))
	return perlCommand(hopStub, base64.StdEncoding.EncodeToString([]byte(strings.Join(argv, "\x00"))))
}

func perlCommand(stub string, args ...string) string {
	parts := []string{"perl", "-e", "'" + stub + "'"}
	for _, a := range args {
		parts = append(parts, quoteWord(a))
	}
	return strings.Join(parts, " ")
}

var (
	binMu    sync.Mutex
	binCache = map[string]*localBuild{}
)

func normPlatform(goos, arch string) (string, string) {
	goos = strings.ToLower(goos)
	switch strings.ToLower(arch) {
	case "x86_64", "amd64":
		arch = "amd64"
	case "aarch64", "arm64":
		arch = "arm64"
	case "armv7l", "armv6l", "arm":
		arch = "arm"
	case "i386", "i686", "386":
		arch = "386"
	}
	return goos, arch
}

type localBuild struct {
	path     string // "" for a build fetched from the wosh client
	fetchKey string // set for a fetched build: platform key to fetch the .xz
	stamp    build.Stamp
	once     sync.Once
	z        []byte
	err      error
	xzOnce   sync.Once
	xzData   []byte
}

// gz returns the gzipped build: from the content-addressed cache
// (~/.cache/wosh/gz/<stamp>.gz, filled by `wosh _warm` or the first upload),
// else compressed now and cached for next time.
func (b *localBuild) gz() ([]byte, error) {
	b.once.Do(func() {
		cache := GzCachePath(b.stamp)
		if z, err := os.ReadFile(cache); err == nil && len(z) > 0 {
			b.z = z
			return
		}
		b.z, b.err = compressFile(b.path)
		if b.err == nil && cache != "" {
			_ = writeAtomic(cache, b.z)
		}
	})
	return b.z, b.err
}

// xz returns the xz-compressed build if it is ready (nil otherwise; then
// gzip is used): from the cache, or fetched from the wosh client. A local
// build missing from the cache is compressed in the background (xz takes
// seconds) so the next upload can use it.
func (b *localBuild) xz() []byte {
	b.xzOnce.Do(func() {
		if b.fetchKey != "" {
			if fb, err := fetchBuild(b.fetchKey + ".xz"); err == nil && fb.stamp == b.stamp {
				b.xzData = fb.z
			}
			return
		}
		cache := cachePath(b.stamp, ".xz")
		if z, err := os.ReadFile(cache); err == nil && len(z) > 0 {
			b.xzData = z
			return
		}
		if cache != "" && b.path != "" {
			go func() {
				if z, err := xzFile(b.path); err == nil {
					_ = writeAtomic(cache, z)
				} else {
					log.Printf("xz %s: %v", b.path, err)
				}
			}()
		}
	})
	return b.xzData
}

// raw returns the uncompressed build.
func (b *localBuild) raw() ([]byte, error) {
	if b.path != "" {
		return os.ReadFile(b.path)
	}
	z, err := b.gz()
	if err != nil {
		return nil, err
	}
	zr, err := gzip.NewReader(bytes.NewReader(z))
	if err != nil {
		return nil, err
	}
	return io.ReadAll(zr)
}

// GzCachePath is where the gzipped build with this stamp is cached.
func GzCachePath(st build.Stamp) string { return cachePath(st, ".gz") }

func cachePath(st build.Stamp, ext string) string {
	dir, err := os.UserCacheDir()
	if err != nil {
		return ""
	}
	return filepath.Join(dir, "wosh", "gz", string(st)+ext)
}

// xzFile compresses with the xz command (much smaller than Go's xz
// packages manage). CRC32 and no BCJ filter keep it decodable by any xz,
// including busybox's.
func xzFile(path string) ([]byte, error) {
	xz, err := exec.LookPath("xz")
	if err != nil {
		return nil, err
	}
	var out, errb bytes.Buffer
	cmd := exec.Command(xz, "-c", "-6", "-T1", "--check=crc32", path)
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%v: %s", err, strings.TrimSpace(errb.String()))
	}
	return out.Bytes(), nil
}

func compressFile(path string) ([]byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	_, _ = zw.Write(raw)
	_ = zw.Close()
	return buf.Bytes(), nil
}

func writeAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := fmt.Sprintf("%s.%d.tmp", path, os.Getpid())
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// Warm pre-compresses wosh binaries into the cache (`wosh _warm FILES...`):
// gzip always, xz when the xz command is available.
func Warm(paths ...string) error {
	_, xzErr := exec.LookPath("xz")
	if xzErr != nil {
		fmt.Println("no xz command: caching gzip only")
	}
	for _, p := range paths {
		st, err := build.Of(p)
		if err != nil {
			return fmt.Errorf("%s: %w", p, err)
		}
		for _, c := range []struct {
			ext string
			fn  func(string) ([]byte, error)
		}{{".gz", compressFile}, {".xz", xzFile}} {
			dst := cachePath(st, c.ext)
			if _, err := os.Stat(dst); err == nil || (c.ext == ".xz" && xzErr != nil) {
				continue
			}
			z, err := c.fn(p)
			if err != nil {
				return fmt.Errorf("%s: %w", p, err)
			}
			if err := writeAtomic(dst, z); err != nil {
				return err
			}
			fmt.Printf("%s -> %s (%.1f MB)\n", p, dst, float64(len(z))/1e6)
		}
	}
	return nil
}

// binaryFor returns a gzipped wosh build for the remote platform. It uses
// this executable when the platform matches, else wosh-<os>-<arch> from
// $WOSH_DIST, <exe dir>/dist, <exe dir> or ~/.cache/wosh.
func binaryFor(goos, arch string) (*localBuild, error) {
	goos, arch = normPlatform(goos, arch)
	key := goos + "-" + arch
	binMu.Lock()
	defer binMu.Unlock()
	if b, ok := binCache[key]; ok {
		return b, nil
	}
	var cands []string
	if goos == runtime.GOOS && arch == runtime.GOARCH {
		if exe, err := os.Executable(); err == nil {
			cands = append(cands, exe)
		}
	}
	name := "wosh-" + key
	if d := os.Getenv("WOSH_DIST"); d != "" {
		cands = append(cands, filepath.Join(d, name))
	}
	if exe, err := os.Executable(); err == nil {
		cands = append(cands, filepath.Join(filepath.Dir(exe), "dist", name), filepath.Join(filepath.Dir(exe), name))
	}
	if home, err := os.UserHomeDir(); err == nil {
		cands = append(cands, filepath.Join(home, ".cache", "wosh", name))
	}
	for _, p := range cands {
		st, err := build.Of(p)
		if err != nil {
			continue
		}
		b := &localBuild{path: p, stamp: st}
		binCache[key] = b
		return b, nil
	}
	if stream.InSession() {
		// chained hop: the wosh client upstream has the builds
		if b, err := fetchBuild(key); err == nil {
			b.fetchKey = key
			binCache[key] = b
			return b, nil
		} else {
			return nil, fmt.Errorf("no wosh build for %s here or at the wosh client: %v", key, err)
		}
	}
	return nil, fmt.Errorf("no wosh build for %s (looked for %s; run `make dist`)", key, strings.Join(cands, ", "))
}

// fetchBuild gets a gzipped build ("<os>-<arch>"), or an xz-compressed one
// ("<os>-<arch>.xz", an error if not ready), from the wosh client of this
// session. Clients without xz support answer the .xz key with an error.
func fetchBuild(key string) (*localBuild, error) {
	s, err := stream.ToClient(wire.StreamSpec{K: "build", Cmd: key})
	if err != nil {
		return nil, err
	}
	defer s.Close()
	b := &localBuild{}
	var z []byte
	for {
		t, p, err := s.Recv()
		if err != nil {
			return nil, err
		}
		switch t {
		case stream.Exit: // stamp
			b.stamp = build.Stamp(p)
		case stream.Data:
			z = append(z, p...)
		case stream.Err:
			return nil, fmt.Errorf("%s", p)
		case stream.Fin:
			b.once.Do(func() { b.z = z })
			return b, nil
		}
	}
}

// serveBuild sends a build to a chained hop that needs one (see fetchBuild).
func serveBuild(ch stream.Conn, key string) {
	key, wantXZ := strings.CutSuffix(key, ".xz")
	goos, arch, _ := strings.Cut(key, "-")
	b, err := binaryFor(goos, arch)
	var z []byte
	if err == nil && wantXZ {
		if z = b.xz(); z == nil {
			err = errors.New("no xz build ready")
		}
	} else if err == nil {
		z, err = b.gz()
	}
	if err != nil {
		_ = ch.Send(stream.Err, []byte(err.Error()))
		_ = ch.Close()
		return
	}
	_ = ch.Send(stream.Exit, []byte(b.stamp))
	_ = ch.Send(stream.Data, z)
	_ = ch.Send(stream.Fin, nil)
	_ = ch.Close()
}

// installed is one wosh found on the remote host by the stub.
type installed struct {
	path     string
	writable bool
	proto    int         // 0: unknown / not wosh
	stamp    build.Stamp // "" if unknown
}

func parseInstalled(fields []string) []installed {
	var out []installed
	for _, f := range fields {
		parts := strings.SplitN(f, "|", 3)
		if len(parts) != 3 {
			continue
		}
		in := installed{path: parts[0], writable: parts[1] == "1"}
		if p, st, ok := strings.Cut(parts[2], "/"); ok {
			in.proto, _ = strconv.Atoi(p)
			in.stamp = build.Stamp(st)
		}
		out = append(out, in)
	}
	return out
}

// decision is the reply to the stub.
type decision struct {
	verb string // USE, UPGRADE, MEM
	path string
	why  string
}

// decide picks what to run on the remote host:
//
//  1. an installed copy of exactly this build;
//  2. upgrade the first installed wosh (PATH order) that is older or
//     protocol-incompatible, if we may write it - never downgrade;
//  3. a protocol-compatible installed wosh (e.g. a newer one);
//  4. otherwise run our build from memory without installing it.
//
// ours is nil when we have no build for the remote platform.
func decide(found []installed, ours *localBuild, force bool) (decision, error) {
	if force {
		if ours == nil {
			return decision{}, errors.New("no build to push for this platform")
		}
		return decision{verb: "MEM", why: "--force-push"}, nil
	}
	if ours != nil {
		for _, in := range found {
			if in.proto == wire.Version && in.stamp == ours.stamp {
				return decision{verb: "USE", path: in.path, why: "same build installed"}, nil
			}
		}
		if len(found) > 0 {
			in := found[0] // the one a plain `wosh` would run
			newer := in.proto != 0 && in.stamp.Time() > ours.stamp.Time()
			if in.writable && !newer {
				return decision{verb: "UPGRADE", path: in.path, why: fmt.Sprintf("installed %s/%s is older", protoStr(in.proto), in.stamp)}, nil
			}
		}
	}
	for _, in := range found {
		if in.proto == wire.Version {
			return decision{verb: "USE", path: in.path, why: "compatible build installed"}, nil
		}
	}
	if ours == nil {
		return decision{}, errors.New("no compatible wosh installed and no build to push for this platform")
	}
	return decision{verb: "MEM", why: "no usable installed wosh"}, nil
}

func protoStr(p int) string {
	if p == 0 {
		return "?"
	}
	return strconv.Itoa(p)
}

// answerStub handles the stub's "WOSHPUSH os arch caps [installed...]" line.
func answerStub(w io.Writer, line string, force bool) (decision, error) {
	f := strings.Fields(line)
	if len(f) < 4 {
		return decision{}, fmt.Errorf("bad push handshake %q", line)
	}
	ours, berr := binaryFor(f[1], f[2])
	d, err := decide(parseInstalled(f[4:]), ours, force)
	if err != nil {
		if berr != nil {
			err = fmt.Errorf("%v (%v)", err, berr)
		}
		return d, err
	}
	if d.verb == "USE" {
		_, err = fmt.Fprintf(w, "USE %s\n", d.path)
		return d, err
	}
	codec, payload, err := choosePayload(ours, f[3])
	if err != nil {
		return d, err
	}
	d.why += fmt.Sprintf("; %s %.1f MB", map[string]string{"x": "xz", "g": "gzip", "r": "raw"}[codec], float64(len(payload))/1e6)
	if d.verb == "UPGRADE" {
		_, err = fmt.Fprintf(w, "UPGRADE %s %d %s\n", d.path, len(payload), codec)
	} else {
		_, err = fmt.Fprintf(w, "MEM %d %s\n", len(payload), codec)
	}
	if err == nil {
		_, err = w.Write(payload)
	}
	return d, err
}

// choosePayload picks the smallest encoding the remote can decode (caps:
// "1" perl can gunzip, "x" xz on PATH): xz if ready, else gzip, else raw.
func choosePayload(b *localBuild, caps string) (codec string, payload []byte, err error) {
	if strings.Contains(caps, "x") {
		if z := b.xz(); z != nil {
			return "x", z, nil
		}
	}
	if strings.HasPrefix(caps, "1") {
		payload, err = b.gz()
		return "g", payload, err
	}
	payload, err = b.raw()
	return "r", payload, err
}
