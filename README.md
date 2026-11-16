# wosh

Roaming remote shell over WebRTC, bootstrapped by ssh.

Like [mosh](https://mosh.org), but the transport is a WebRTC data channel
(ICE with STUN/TURN, DTLS-encrypted, via [pion](https://github.com/pion/webrtc)),
and signaling travels over whatever path already reaches the host: an ssh
chain through any number of jump hosts, or the terminal of another wosh
session.

```
Init:    client -ssh-> jump1 -ssh-> jump2 -ssh-> jumpN -ssh-> dest
Upgrade: client ===========wosh (WebRTC)==================> dest

Init:    client =wosh=> dest1 -ssh-> dest2          (then run `wosh up` on dest2)
Upgrade: client ==========wosh=====> dest2
```

## Usage

Only the client needs wosh: it streams itself to the destination (see
below). Jump hosts need nothing.

```sh
wosh -J jump1,jump2,jumpN user@dest     # any ssh args; wosh flags are --long
# inside that session:
ssh dest2
wosh up                                  # client asks, then connects directly to dest2
exit                                     # back to dest1's session
```

Flags: `--stun=`, `--turn=URL|USER|PASS`, `--relay` (TURN only), `--server-cmd=`
(remote wosh path), `--yes` (auto-accept upgrades), `--log=FILE`,
`--resume=ID:KEY`. Escapes after Enter: `~.` quit, `~&` detach, `~s` stack, `~?` help.
`kill -USR1 <client pid>` forces every link to reconnect (simulated roam).

Remote login shell can be anything (bash, zsh, fish, tcsh, nushell, ...): the
ssh remote command is built from syntax all shells parse identically and
runs wosh via `sh -c`. Sessions start `$SHELL` (fallback: `/etc/passwd`) as a login shell.

## No install on the remote (default)

```sh
make dist            # builds bin/dist/wosh-<os>-<arch>
bin/wosh -J jump1,jump2 user@dest
```

wosh pushes itself by default. `--no-push` (or `WOSH_PUSH=0`, or an
explicit `--server-cmd=`) uses an installed wosh instead; if the remote
has no perl or there is no build for its platform, the client falls back
to an installed wosh automatically.

The remote command is a perl one-liner (perl ships with Linux distros and
macOS). It reports `uname`, receives the matching gzipped build (~4 MB)
over the same ssh stdin and executes it:

* **Linux**: from a `memfd` - never written to any filesystem
  (`/proc/<pid>/exe -> /memfd:wosh (deleted)`).
* **macOS / memfd blocked**: written to `/dev/shm`, `$TMPDIR`, `/tmp` or
  `$HOME` (first that works), executed, and deleted as soon as the session
  daemon is running (about a second on disk).

Reconnects never re-upload: a tiny perl relay talks to the daemon's control
socket. Builds are looked up in `$WOSH_DIST`, `<exe dir>/dist`, `<exe dir>`
and `~/.cache/wosh` (`wosh-<os>-<arch>`); the client's own binary is used for
its own platform. Note: exec from memfd is a known "fileless" technique and
may be flagged by EDR/security tooling; `vm.memfd_noexec=2` disables it.

## How it works

* **Signaling** (`internal/wire`): host->client messages are a private OSC
  escape (`ESC ]5379;wosh;<base64 json> BEL`); client->host messages are an
  input line `WOSH:<base64 json>\r`. Both pass unmodified through ssh hops
  and PTYs, so the same protocol works over `ssh -T` pipes and in-band
  through a running session's terminal.
* **Daemon** (`wosh _daemon`, detached, one per session): owns the PTY,
  keeps a 4 MiB output ring addressed by absolute offset, accepts offers on
  a unix socket (`$XDG_RUNTIME_DIR/wosh/ID.sock`, 0700), replaces the peer
  connection on every reconnect.
* **Relays**: `wosh server` (initial, over ssh, exits after answering),
  `wosh attach ID` (reconnect over ssh), `wosh up` (nested: stays parked in
  the parent terminal so the child can be re-signaled through it).
* **Client** (`internal/client`): a stack of sessions; keyboard goes to the
  top. Output and input carry stream offsets; unacknowledged input is
  resent and missed output replayed after reconnect, so nothing is lost.
* **Roaming**: 1 s pings (6 s timeout), ICE failure, or a local address
  disappearing trigger reconnect. Root sessions re-run ssh (`BatchMode`) with
  `attach`; nested sessions wait for their parent, then signal through it.

## Security notes

* The DTLS fingerprint in the SDP is exchanged over ssh (or an existing
  wosh session), so the WebRTC link is authenticated end to end. Each
  session also has a random key checked on every reconnect.
* Any program printing to the terminal can emit a `hello`; the client
  therefore **asks** before upgrading (unless `--yes`) and only reacts to
  the foreground session.
* STUN reveals your public IP to the STUN server; `--relay` forces TURN.
  TURN credentials given to the client are passed to the remote daemons.

## Limitations / TODO

* Reconnecting the root needs non-interactive ssh auth (keys/agent).
* Pushing covers the root host only; nested hops still need `wosh up`
  (in-band push through the terminal is planned).
* tmux/screen on an intermediate hop swallow unknown OSC (use passthrough).
* No predictive local echo (mosh's SSP); no IPv6-only/TURN-TCP tuning yet.
* Sessions detached with `~&` can only be resumed at the root (`--resume`).

## Tests

`go test ./...` — unit tests plus an end-to-end test driving real daemons
locally: bootstrap, nested `wosh up`, forced root and child reconnects
(with input typed while down and 3000 lines of output mid-drop), and stack
unwinding on exit; plus the remote command checked under every installed shell.
