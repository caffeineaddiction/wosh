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

## Remote binary: installed, upgraded, or pushed

Only the client needs wosh. On connect, a perl one-liner on the remote
reports the platform and every installed wosh it finds (`$PATH`, `~/bin`,
`~/.local/bin`, `~/go/bin`) with its build stamp (`wosh _proto`:
protocol / commit time / binary hash). The client then decides:

1. **same build installed** - run it (no upload; ~0.4 s to a prompt);
2. **older build installed and writable** - upgrade it in place (atomic
   rename; sessions running the old file are unaffected), then run it;
3. **newer but protocol-compatible** - run it (never downgrade);
4. otherwise **run this build from memory** without installing it: Linux
   `memfd` (never on disk); elsewhere (macOS) a temp file deleted right
   after start.

Builds for other platforms come from `make dist` (`bin/dist/wosh-<os>-<arch>`;
also `$WOSH_DIST`, `~/.cache/wosh`). Uploads are xz-compressed (~3.7 MB
for linux-amd64) when the remote has an `xz` command, else gzipped
(~4.8 MB, perl's built-in gunzip), else raw. Compressed copies are cached
by stamp in `~/.cache/wosh/gz/` (`wosh _warm FILES` / `make dist`
pre-fills both); xz needs the `xz` command on the client, and a build
without a cached `.xz` is sent gzipped once while xz compresses it in the
background.

`--force-push` always runs from memory; `--no-push` (or `WOSH_PUSH=0`,
or `--server-cmd=`) never uploads. Without remote perl, the client falls
back to an installed wosh. Reconnects never upload: a tiny perl relay
talks to the daemon's control socket. Memfd exec is a known "fileless"
technique and may be flagged by EDR; `vm.memfd_noexec=2` disables it.

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
* **Shared sessions**: a client that sends a client id (`cid`, stable across
  its reconnects) can share a session with other such clients. One of them
  controls it (input, resize, kill, forwards); the others watch the same
  output. An offer with `join` only takes control if nobody has it; a
  watcher asks for control with a `{"op":"take"}` control frame, and the
  daemon tells each peer its role (`{"op":"role","role":"control"|"view"}`).
  Before any output, each peer gets `{"op":"info","out":N}`: output below
  offset N is replay, so a terminal should not answer queries (DSR, DA)
  found there; they were answered when they were new.
  When the controller is gone for 10 s, the longest watcher gets control.
  An offer with `watch` (and a cid) is a passive peer, such as wash's
  recorder: it always watches, is never promoted and cannot take control.
  The controller sets the terminal size; peers with a cid are told it
  (`{"op":"size","cols","rows"}`) when they connect and on every change.
  A client without a cid (the `wosh` CLI) always takes control and the peer
  it replaces is dropped, as before.
* **Cleanup**: a daemon whose control socket was deleted (its runtime dir
  removed, e.g. by a test or a logout without linger) can never be reached
  again; once no client is connected either, it hangs up the shell and exits
  after 5 s.
* **Roaming**: 1 s pings (6 s timeout), ICE failure, or a local address
  disappearing trigger reconnect. Root sessions re-run ssh (`BatchMode`) with
  `attach`; nested sessions wait for their parent, then signal through it.

## Typing on slow links (predictive echo)

Like mosh, wosh echoes what you type locally before the server confirms
it. A terminal emulator mirrors the remote screen (cells, widths, cursor),
and keystrokes are applied to a predicted copy of the screen:

| keys | shells (readline, fish, ...) | vim / nano (alternate screen) |
|---|---|---|
| printable | insert at the cursor | insert at the cursor |
| Backspace, Delete | delete, shift the rest of the line | same |
| Left / Right | move the cursor | same (incl. `ESC O D/C`) |
| Home / End, Ctrl-A / Ctrl-E | start of input / end of text | column 0 / end of text (Ctrl-A/E not predicted) |
| Alt-B, Ctrl-Left / Alt-F, Ctrl-Right | previous word start / next word end | previous / next word start |
| Ctrl-W, Alt-Backspace | delete back to whitespace / previous word | not predicted |
| Ctrl-K / Ctrl-U | kill to end of text / back to start of input | not predicted |
| Up / Down | not predicted (history) | move a line, keeping the wanted column |

The "start of input" is learned from the cursor at the first key on a fresh
prompt (after Enter, Ctrl-C, Ctrl-L or a Ctrl-U that was not predicted). In
vim's normal mode the cursor is kept on the last character of short lines;
with `-- INSERT --` showing, or in nano, it may sit after the text. Changed cells and the
predicted cursor are drawn immediately - typed text underlined while the
round trip is > 80 ms or the link is down.

The server sends echo acks (as mosh does): which input the application
has had at least 50 ms to answer, after the output it produced. Only then
is a prediction judged - typed cells must show the typed character and the
cursor must be where predicted - so text an app draws by itself (fish
autosuggestions) never confirms a keystroke; anything else is withdrawn.
Each stretch of typing starts tentative: typed text is shown once a typed
character of it has been confirmed, cursor movement once a predicted
cursor position has been - so password prompts and editor commands (vim
normal mode) never show predicted text, while navigation does. Enter, Tab,
shell Up/Down and other control keys are not predicted, and nothing after
them is until the server has processed them. Apps with different word
rules (fish treats punctuation as word boundaries) just get their
predictions withdrawn after one round trip, as does zsh's Ctrl-U (which
kills the whole line). Ctrl-K/Ctrl-U are not predicted on input that wraps
past the right margin. Wide characters (CJK, emoji) and wrapping at
the right margin (not on the bottom row) are handled.

`--predict=adaptive` (default: when the smoothed round trip is > 30 ms or
the link is down), `always`, `never`; env `WOSH_PREDICT`. Unlike mosh, the
raw output still reaches your terminal (scrollback works); predictions
are drawn with relative cursor moves only. `~s` shows each session's rtt.

Keystrokes are never lost: input is offset-acknowledged and resent after
reconnects, and keys typed while a chained hop is being set up are
forwarded to the new session in order (typeahead).

## Inside a session

* **Environment**: `LANG`, `LC_*`, `COLORTERM` are forwarded; `--env=K=V`
  adds more. `WOSH_SESSION`, `WOSH_SOCK`, `WOSH_BIN`, `WOSH_BIN_DIR` are set.
* **wosh on PATH**: a per-session bin dir (in the runtime dir, a tmpfs on
  most Linux systems) is prepended to `PATH` with `wosh`, `ssh`, `scp`,
  `sftp`. On Linux they point at `/proc/<daemon>/exe`, so a pushed,
  memory-only wosh is usable from the CLI without touching disk.
* **Chained hops**: `wosh hostC` inside a session on B does the ssh from B
  (C may be reachable only from B), then hands C to your client, which
  connects to C directly (authenticated by B's session token, no prompt;
  the build for C's platform is fetched from your client if B lacks it).
  B then spins down: its session, forwards and connections end. C is
  reconnected by ssh'ing through the same hosts (`ssh B` -> B's own
  `ssh C` with B's config/keys -> reattach; needs only perl+ssh on B).
* **`src:` = your client**: `rsync -a src:dir/ .`, `scp f src:`,
  `sftp src`. Transfers (rsync --server, scp -t/-f, sftp subsystem) run on
  the client over the wosh channel after a once-per-session prompt
  (`--allow-src` skips it), in your home directory. Anything else (e.g.
  `ssh src`) falls back to real ssh into the client's sshd, tunnelled via
  wosh. `--src=NAME` renames the alias.
* **Port forwarding / agent**: `-L`, `-R`, `-D`, `-A` with ssh syntax.
  They always point at the current destination: after a chained hop the
  -L/-D listeners send new connections to the new host, -R listeners move
  there, and the agent socket is offered there. Forwarded connections
  (VNC, SOCKS, agent, src: transfers) survive roaming between the same
  two endpoints: streams are sequenced, acknowledged and resumed on the
  new link.

If your shell rc rebuilds `PATH` or sets `SSH_AUTH_SOCK` (common with fish),
the session bin dir may end up behind `/usr/bin` and the forwarded agent
hidden. `wosh --fix-shell` adds a managed block to the end of your rc that
restores both; by hand:

```sh
# fish
set -q WOSH_BIN_DIR; and fish_add_path --prepend --move $WOSH_BIN_DIR
# bash / zsh
[ -n "$WOSH_BIN_DIR" ] && PATH="$WOSH_BIN_DIR:$PATH"
```

## macOS remotes: Local Network privacy

macOS lets a process started over ssh reach the LAN only while that ssh
session exists; once orphaned, its sends to LAN addresses fail with
`no route to host` (incoming traffic still works, so ICE can pick a LAN
pair that then dies). wosh handles this:

* for macOS hosts the client keeps the bootstrap ssh open (idle) as an
  anchor, so the direct LAN path stays usable;
* when the anchor goes away (roaming, ssh dropped) the daemon closes the
  peer immediately, before the block kicks in, so the client renegotiates
  in ~0.2 s instead of timing out;
* before answering, a macOS daemon probes the client's on-link candidates
  and drops the ones it cannot send to, so ICE picks a working path.

Debugging: `WOSH_SIM_DELAY=250ms` (client delays everything it receives,
to try high-latency behaviour such as prediction on a fast link),
`WOSH_DEBUG=1` (verbose pion logs), `WOSH_ICE_NET=udp4|udp6`
(restrict candidates), `WOSH_ICE_IFACES=lo,tailscale0` (gather candidates
on these interfaces only, like `wosh _daemon --ice-iface`; session shells
inherit it), `WOSH_KEEP_LOGS=1` (keep daemon logs); `~s` shows
the selected candidate pair.

`wosh _daemon --ice-iface tailscale0,lo` keeps a daemon's ICE candidates,
and the UDP sockets behind them, to the named interfaces: no LAN or
container addresses in the answer. A loopback interface in the list also
enables loopback candidates. Each candidate appears twice in an answer
(ICE components 1 and 2); both lines name the same socket.

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
* Hops reached with plain ssh still need `wosh up` and an installed wosh;
  use `wosh host` from inside a session instead (chained hop).
* Word movement and Ctrl-U follow readline rules in shells; fish's word
  rules and zsh's Ctrl-U differ (mispredictions are withdrawn, not left on
  screen).
* A chained hop ends the intermediate host's forwarded connections (by
  design); new connections go to the new destination.
* tmux/screen on an intermediate hop swallow unknown OSC (use passthrough).
* No IPv6-only/TURN-TCP tuning yet.
* Sessions detached with `~&` can only be resumed at the root (`--resume`).

## Tests

`go test ./...` — unit tests plus an end-to-end test driving real daemons
locally: bootstrap, nested `wosh up`, forced root and child reconnects
(with input typed while down and 3000 lines of output mid-drop), and stack
unwinding on exit; plus the remote command checked under every installed shell.
