// Command wosh is a roaming remote shell: bootstrap over ssh (through any
// number of jump hosts), then upgrade to a direct WebRTC data channel
// (ICE with STUN/TURN) that survives network changes.
package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"golang.org/x/term"

	"wosh/internal/build"
	"wosh/internal/client"
	"wosh/internal/predict"
	"wosh/internal/rtc"
	"wosh/internal/server"
	"wosh/internal/shellfix"
	"wosh/internal/shim"
	"wosh/internal/stream"
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

  wosh --fix-shell[=SHELL] [--no-reload]
      Make the session bin dir (wosh, ssh/scp/sftp for src:) win in PATH
      even if your shell rc rebuilds PATH: appends a managed block to the
      end of the rc file(s) of $SHELL (fish, bash, zsh, sh, tcsh), backs
      the file up once (*.wosh.bak) and, inside a wosh session, reloads
      the shell (exec $SHELL -l). --unfix-shell removes the block.

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
  -L [bind:]port:host:hostport   forward local port to host:hostport (dialed remotely)
  -R [bind:]port:host:hostport   forward remote port to host:hostport (dialed here)
  -D [bind:]port                 local SOCKS5 proxy through the remote host
  -A                             forward your ssh-agent (SSH_AUTH_SOCK)
      Forwards always point at the current destination: after a chained
      hop they move to the new host; connections survive roaming.
  --env=NAME=VALUE        set a variable in remote sessions (LANG, LC_*,
                          COLORTERM are forwarded automatically)
  --src=NAME              host name meaning "this client" inside sessions
                          (default src: rsync src:dir ., scp f src:, sftp src)
  --allow-src             serve src: transfers without asking
  --predict=MODE          local echo of typed text before the server
                          confirms it (like mosh): adaptive (default, when
                          the round trip is > 30ms or the link is down),
                          always, never (env WOSH_PREDICT)
  --resume=ID:KEY         reattach to a detached root session
  --yes                   accept in-band upgrade requests without asking
  --log=FILE              debug log
  --local=CMD             testing: run CMD locally instead of ssh

inside a session:
  wosh host      chained hop: connect this client directly to host (ssh'd
                 from here), this session idles until host's session ends
  wosh, ssh, scp, sftp are on PATH; host "src" is the wosh client

escapes (after Enter): ~. quit  ~& detach  ~s sessions  ~? help
`

func main() {
	// session bin dir links: ssh/scp/sftp stand-ins (see internal/shim)
	switch filepath.Base(os.Args[0]) {
	case "ssh":
		os.Exit(shim.SSH(os.Args[1:]))
	case "scp":
		os.Exit(shim.SCP(os.Args[1:]))
	case "sftp":
		os.Exit(shim.SFTP(os.Args[1:]))
	}
	args := os.Args[1:]
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	if a := args[0]; a == "fix-shell" || a == "--fix-shell" || strings.HasPrefix(a, "--fix-shell=") || a == "--unfix-shell" {
		os.Exit(fixShell(args))
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
	case "_proxy":
		os.Exit(shim.Proxy())
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
	client.KillChildren()
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
		case "--own-binary":
			i++
			o.OwnBinary = args[i]
		case "--ice-iface":
			if i+1 >= len(args) {
				fatal("--ice-iface needs interface names, e.g. tailscale0,lo")
			}
			i++
			o.ICEInterfaces = rtc.SplitList(args[i])
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
	// never leave a bootstrap ssh behind: not on return, not when killed
	defer client.KillChildren()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		s := <-sigs
		client.KillChildren()
		os.Exit(128 + int(s.(syscall.Signal)))
	}()
	o := client.Options{ServerCmd: os.Getenv("WOSH_SERVER_CMD")}
	stun, turn := os.Getenv("WOSH_STUN"), os.Getenv("WOSH_TURN")
	// push by default; an explicit --server-cmd means "use the installed one"
	o.Push = os.Getenv("WOSH_PUSH") != "0" && o.ServerCmd == ""
	if m, err := predict.ParseMode(os.Getenv("WOSH_PREDICT")); err == nil {
		o.Predict = m
	}
	logFile := ""
	var sshArgs []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			sshArgs = append(sshArgs, args[i+1:]...)
			break
		}
		if a == "-A" { // agent forwarding over wosh, not the bootstrap ssh
			o.Agent = true
			continue
		}
		// -L/-R/-D are wosh's (forwarded over WebRTC), not the bootstrap ssh's
		if len(a) >= 2 && a[0] == '-' && strings.IndexByte("LRD", a[1]) >= 0 {
			spec := a[2:]
			if spec == "" && i+1 < len(args) {
				i++
				spec = args[i]
			}
			f, err := client.ParseForward(a[1], spec)
			if err != nil {
				fatal("%v", err)
			}
			o.Forwards = append(o.Forwards, f)
			continue
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
		case "env":
			k, val, ok := strings.Cut(v, "=")
			if !ok || k == "" {
				fatal("--env wants NAME=VALUE")
			}
			if o.Env == nil {
				o.Env = map[string]string{}
			}
			o.Env[k] = val
		case "src":
			o.Src = v
		case "allow-src":
			o.AllowSrc = true
		case "predict":
			m, err := predict.ParseMode(v)
			if err != nil {
				fatal("%v", err)
			}
			o.Predict = m
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

	if os.Getenv("WOSH_SESSION") != "" && os.Getenv("WOSH_NO_CHAIN") == "" && term.IsTerminal(int(os.Stdin.Fd())) {
		// inside a wosh session: hand the new host to the client upstream
		// so it connects directly (chained hop) instead of nesting clients
		if bin := os.Getenv("WOSH_BIN_DIR"); bin != "" && o.SSH == "" {
			o.SSH = shimFreeSSH()
		}
		if len(o.Forwards) > 0 {
			fmt.Fprintln(os.Stderr, "wosh: -L/-R/-D are not supported on chained hops yet; ignoring")
		}
		code, err := client.Delegate(o)
		if err != nil {
			fmt.Fprintf(os.Stderr, "wosh: %v\n", err)
			return 255
		}
		return code
	}
	code, err := client.Run(o)
	if err != nil {
		fmt.Fprintf(os.Stderr, "wosh: %v\n", err)
		return 255
	}
	return code
}

// shimFreeSSH: the real ssh, not our stand-in (avoids a needless hop).
func shimFreeSSH() string {
	bin := os.Getenv("WOSH_BIN_DIR")
	for _, dir := range filepath.SplitList(os.Getenv("PATH")) {
		if dir == bin || dir == "" {
			continue
		}
		p := filepath.Join(dir, "ssh")
		if st, err := os.Stat(p); err == nil && !st.IsDir() {
			if r, _ := filepath.EvalSymlinks(p); r != "" && filepath.Base(r) == "ssh" {
				return p
			}
		}
	}
	return "ssh"
}

func fixShell(args []string) int {
	shell, reload, undo := "", true, false
	for _, a := range args {
		switch {
		case a == "--no-reload":
			reload = false
		case a == "--unfix-shell":
			undo = true
		case strings.HasPrefix(a, "--fix-shell="):
			shell = strings.TrimPrefix(a, "--fix-shell=")
		}
	}
	if shell == "" {
		shell = os.Getenv("SHELL")
	}
	if shell == "" {
		shell = server.LoginShell()
	}
	targets, err := shellfix.Targets(shell)
	if err != nil {
		fatal("%v", err)
	}
	changed := false
	for _, t := range targets {
		if undo {
			ch, err := shellfix.Remove(t)
			if err != nil {
				fatal("%s: %v", t.File, err)
			}
			if ch {
				fmt.Printf("wosh: removed block from %s\n", t.File)
			}
			continue
		}
		ch, bak, err := shellfix.Apply(t)
		if err != nil {
			fatal("%s: %v", t.File, err)
		}
		switch {
		case ch && bak != "":
			fmt.Printf("wosh: updated %s (original saved as %s)\n", t.File, bak)
		case ch:
			fmt.Printf("wosh: created %s\n", t.File)
		default:
			fmt.Printf("wosh: %s is already up to date\n", t.File)
		}
		changed = changed || ch
	}
	if undo || !reload {
		return 0
	}
	bin := os.Getenv("WOSH_BIN_DIR")
	first := strings.SplitN(os.Getenv("PATH"), string(os.PathListSeparator), 2)[0]
	if !changed && bin != "" && first == bin {
		return 0 // already effective in this shell
	}
	sh := shell
	if p, err := exec.LookPath(shell); err == nil {
		sh = p
	}
	reloadCmd := "exec " + sh + " -l"
	if os.Getenv("WOSH_SOCK") != "" && term.IsTerminal(int(os.Stdin.Fd())) {
		// we cannot reload the shell that started us, but the session
		// daemon can type for us: the shell runs it once we exit
		if err := stream.TypeIntoSession(reloadCmd + "\r"); err == nil {
			fmt.Println("wosh: reloading your shell...")
			return 0
		}
	}
	fmt.Printf("wosh: reload your shell to apply: %s\n", reloadCmd)
	return 0
}
