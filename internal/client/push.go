package client

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

// Binary push: run wosh on hosts where it is not installed.
//
// The remote command is a perl one-liner (perl ships with practically every
// Linux distribution and with macOS). It reports the platform, receives a
// gzipped wosh build for it over the same ssh stdin and executes it:
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

const pushStub = `$|=1;$o=$^O;$m=` + "`uname -m`" + `;chomp $m;` +
	`$z=eval{require IO::Uncompress::Gunzip;1}?1:0;` +
	`print "WOSHPUSH $o $m $z\n";binmode STDIN;$l=<STDIN>;defined $l or exit 1;chomp $l;` +
	`$d="";while(length($d)<$l){$r=read(STDIN,$d,$l-length($d),length($d));$r or die "wosh push: short read\n"}` +
	`if($z){$u="";IO::Uncompress::Gunzip::gunzip(\$d=>\$u) or die "wosh push: gunzip failed\n";$d=$u}` +
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

func perlCommand(stub string, args ...string) string {
	parts := []string{"perl", "-e", "'" + stub + "'"}
	for _, a := range args {
		parts = append(parts, quoteWord(a))
	}
	return strings.Join(parts, " ")
}

var (
	binMu    sync.Mutex
	binCache = map[string][]byte{}
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

// binaryFor returns a gzipped wosh build for the remote platform. It uses
// this executable when the platform matches, else wosh-<os>-<arch> from
// $WOSH_DIST, <exe dir>/dist, <exe dir> or ~/.cache/wosh.
func binaryFor(goos, arch string) ([]byte, error) {
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
		raw, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		var buf bytes.Buffer
		zw, _ := gzip.NewWriterLevel(&buf, gzip.BestCompression)
		_, _ = zw.Write(raw)
		_ = zw.Close()
		binCache[key] = buf.Bytes()
		return buf.Bytes(), nil
	}
	return nil, fmt.Errorf("no wosh build for %s (looked for %s; run `make dist`)", key, strings.Join(cands, ", "))
}

// sendBinary answers the push stub's "WOSHPUSH os arch gz" line.
func sendBinary(w io.Writer, line string) error {
	f := strings.Fields(line)
	if len(f) < 4 {
		return fmt.Errorf("bad push handshake %q", line)
	}
	gz, err := binaryFor(f[1], f[2])
	if err != nil {
		return err
	}
	payload := gz
	if f[3] != "1" { // remote perl lacks IO::Uncompress::Gunzip
		zr, err := gzip.NewReader(bytes.NewReader(gz))
		if err != nil {
			return err
		}
		if payload, err = io.ReadAll(zr); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(w, "%d\n", len(payload)); err != nil {
		return err
	}
	_, err = w.Write(payload)
	return err
}
