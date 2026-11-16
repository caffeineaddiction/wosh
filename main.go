// Command wosh is a roaming remote shell: bootstrap over ssh (through any
// number of jump hosts), then upgrade to a direct WebRTC data channel
// (ICE with STUN/TURN) that survives network changes.
package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"wosh/internal/build"
	"wosh/internal/client"
	"wosh/internal/rtc"
	"wosh/internal/server"
	"wosh/internal/wire"
)

var version = "0.1.0-dev"

const usage = `wosh - roaming shell over WebRTC, bootstrapped by ssh

usage:
  wosh [--wosh-flags] [ssh args...] destination
      Connect: run "wosh server" on destination via ssh (jump hosts, ssh
      config, agent all work as usual), then switch to a direct WebRTC link.
      e.g.  wosh -J jump1,jump2,jumpN user@dest

  wosh up [-- command...]
      Run inside a wosh session, on a host you reached with plain ssh
      (client -wosh-> dest1 -ssh-> dest2). The client upgrades to a direct
      link to this host; this terminal stays parked as the signaling path.

  wosh server | wosh attach ID     (remote side, used by the client)
  wosh version

client flags (always --long so they never collide with ssh flags):
  --stun=URL[,URL]        STUN servers (default stun:stun.l.google.com:19302; env WOSH_STUN)
  --turn=URL|USER|PASS    TURN server(s), ';' separated (env WOSH_TURN)
  --relay                 force TURN relay (hide IP addresses)
  Default remote start: use an installed wosh (PATH, ~/bin, ~/.local/bin,
  ~/go/bin) if it is this build; upgrade it in place if it is older and
  writable; use it if it is newer but compatible; otherwise upload this
  build over ssh and run it from memory (Linux memfd; elsewhere a temp file
  deleted right after start). Needs remote perl; without it, an installed
  wosh is tried.
  Builds for other platforms: make dist (bin/dist/wosh-<os>-<arch>).
  --force-push            run this build from memory; ignore/keep installed wosh
  --no-push               never upload; installed wosh only (env WOSH_PUSH=0)
  --server-cmd=PATH       installed wosh on remote hosts, implies --no-push
                          (default: wosh in PATH, ~/.local/bin, ~/go/bin;
                          env WOSH_SERVER_CMD). Works with any login shell.
  --ssh=PATH              ssh binary (default ssh)
  --resume=ID:KEY         reattach to a detached root session
  --yes                   accept in-band upgrade requests without asking
  --log=FILE              debug log
  --local=CMD             testing: run CMD locally instead of ssh

escapes (after Enter): ~. quit  ~& detach  ~s sessions  ~? help
`

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	switch args[0] {
	case "server":
		relay(server.ModeServer, "", args[1:])
	case "up":
		if os.Getenv("WOSH_SESSION") == "" && os.Getenv("SSH_CONNECTION") == "" {
			fmt.Fprintln(os.Stderr, "wosh up: warning: not obviously inside a wosh/ssh session")
		}
		relay(server.ModeUp, "", args[1:])
	case "attach":
		if len(args) < 2 {
			fatal("usage: wosh attach ID")
		}
		relay(server.ModeAttach, args[1], nil)
	case "_daemon":
		daemon(args[1:])
	case "version", "--version":
		fmt.Println("wosh", version, "protocol", wire.Version, "build", build.Self())
	case "_warm": // pre-compress builds for upload: wosh _warm bin/wosh bin/dist/*
		if err := client.Warm(args[1:]...); err != nil {
			fatal("%v", err)
		}
	case "_proto":
		fmt.Printf("%d/%s\n", wire.Version, build.Self())
	case "help", "--help", "-h":
		fmt.Print(usage)
	default:
		os.Exit(runClient(args))
	}
}

func fatal(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "wosh: "+format+"\n", a...)
	os.Exit(1)
}

func relay(mode server.RelayMode, id string, rest []string) {
	var cmd []string
	if len(rest) > 0 && rest[0] == "--" {
		cmd = rest[1:]
	}
	err := server.RunRelay(server.RelayOptions{Mode: mode, ID: id, Command: cmd, Linger: 24 * time.Hour})
	if err != nil {
		fatal("%v", err)
	}
}

func daemon(args []string) {
	var o server.DaemonOptions
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--id":
			i++
			o.ID = args[i]
		case "--linger":
			i++
			d, err := time.ParseDuration(args[i])
			if err != nil {
				fatal("bad --linger")
			}
			o.Linger = d
		case "--":
			o.Command = args[i+1:]
			i = len(args)
		}
	}
	o.ReadyFD = os.NewFile(3, "ready")
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	rtc.LogWriter = os.Stderr
	if err := server.RunDaemon(o); err != nil {
		log.Fatal(err)
	}
}

func runClient(args []string) int {
	o := client.Options{ServerCmd: os.Getenv("WOSH_SERVER_CMD")}
	stun, turn := os.Getenv("WOSH_STUN"), os.Getenv("WOSH_TURN")
	// push by default; an explicit --server-cmd means "use the installed one"
	o.Push = os.Getenv("WOSH_PUSH") != "0" && o.ServerCmd == ""
	logFile := ""
	var sshArgs []string
	for i, a := range args {
		if a == "--" {
			sshArgs = append(sshArgs, args[i+1:]...)
			break
		}
		if !strings.HasPrefix(a, "--") {
			sshArgs = append(sshArgs, a)
			continue
		}
		k, v, _ := strings.Cut(a[2:], "=")
		switch k {
		case "stun":
			stun = v
		case "turn":
			turn = v
		case "relay":
			o.RelayOnly = true
		case "server-cmd":
			o.ServerCmd = v
			o.Push = false
		case "ssh":
			o.SSH = v
		case "resume":
			o.Resume = v
		case "push":
			o.Push = true
		case "no-push":
			o.Push = false
		case "force-push":
			o.Push, o.ForcePush = true, true
		case "yes":
			o.Auto = true
		case "log":
			logFile = v
		case "local":
			o.Local = v
		default:
			fatal("unknown flag --%s (ssh flags use a single dash)", k)
		}
	}
	if len(sshArgs) == 0 && o.Local == "" {
		fmt.Fprint(os.Stderr, usage)
		return 2
	}
	o.SSHArgs = sshArgs
	o.ICE = rtc.ParseICE(stun, turn)

	log.SetOutput(io.Discard)
	if logFile != "" {
		f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			fatal("%v", err)
		}
		log.SetOutput(f)
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
		rtc.LogWriter = f
	}

	code, err := client.Run(o)
	if err != nil {
		fmt.Fprintf(os.Stderr, "wosh: %v\n", err)
		return 255
	}
	return code
}
