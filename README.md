# mcholepunch

mcholepunch is a Java 17 library that turns a Minecraft login handshake into an encrypted byte
stream owned by your code. A modded client opens a connection that reads as a vanilla login, a
modded server claims it before vanilla builds a login listener, the vanilla RSA/AES exchange runs
with the server's own keypair, and from there the two mods talk freely. Vanilla never parses a
packet of it, and an unmarked login replays to vanilla untouched so normal players join normally.

The library is version independent. You supply the vanilla protocol number for the handshake, and
every other byte mcholepunch sends or parses is the same on every Minecraft version. Nothing in
this repository tracks Minecraft releases, and there is no Minecraft code and no mixin here.

```java
// server, once at startup: what a claimed login becomes
NettyChannelRegistry.register(maximumPendingWriteBytes, (channel, connection) -> {
	// install your handlers on the taken-over channel; inbound application bytes arrive on it,
	// outbound writes travel the pipeline toward the connection
	channel.pipeline().addLast("application", new MyProtocolHandler(connection));
	connection.write(ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8)));
});

// server, in the Netty pipeline of every accepted Minecraft channel, before vanilla can own the login:
NettyLoginClaimHandler.install(channel.pipeline(), serverKeyPair);

// client; the protocol version is the vanilla protocol number of the target server
HolepunchConnection connection = HolepunchClient.connect(
		HolepunchClient.resolve("mc.example.com", 25565).toCompletableFuture().get(),
		763,
		handler,
		HolepunchOptions.builder().handshakeHostSuffix(HolepunchOptions.FORGE_FML3_HANDSHAKE_HOST_SUFFIX).build()
).toCompletableFuture().get(20, TimeUnit.SECONDS);
```

AutoModpack uses this to run its modpack protocol through the one port a modded server is
guaranteed to have open. It layers its own TLS on top with the transport handoff described below.

When the caller already knows the transport address, build the route with
`HolepunchRoute.at(handshakeHost, handshakePort, transportAddress)` instead of calling `resolve`.
The handshake still presents the logical host and port; only the socket changes.

## How it works

1. The client resolves the address the way Minecraft does (SRV records, IPv6 literals) and opens a
   TCP connection to it.
2. It sends a Handshake (packet 0, login intention) carrying the protocol number you supplied, then
   a Login Start (packet 0). On every version the Login Start is `name + UUID`, the newest shape,
   because the marker hides in the UUID and no vanilla decoder ever sees the packet.
3. `NettyLoginClaimHandler` holds the handshake frame, reads the Login Start, and checks the UUID.
   No marker: it replays the original bytes downstream and removes itself, and the login proceeds
   exactly as vanilla would have run it. Marker: it starts the login negotiator, which strips the
   vanilla pipeline wholesale and replaces it with mcholepunch's own vanilla-shaped framing,
   before vanilla builds a login listener.
4. The server sends an Encryption Request (packet 1, `shouldAuthenticate` always false). The
   client answers with a fresh AES secret sealed to the server's RSA public key, and both sides
   switch to AES/CFB8.
5. The server sends an encrypted Custom Query (packet 4), `mcholepunch:takeover`, and once that
   write lands it swaps the negotiation framing for its transport and hands the channel to the
   registered `HolepunchChannelApplication`. The client switches when it receives the query; from
   that point the client delivers `ByteBuffer`s to its `HolepunchHandler` and the server owns the
   taken-over Netty channel.

### The marker

A marker UUID is built by `HolepunchMarker`: 122 payload bits packed into an RFC 4122 v4-shaped
UUID. The payload is an 80-bit constant (SHA-256 of `mcholepunch:marker-hash`, truncated, by
default), a version byte, a flags byte, and a 26-bit random nonce. The first 96 bits are XOR-masked
with SHA-256 of `mcholepunch:uuid-mask` keyed by the nonce, so the constant does not appear in the
clear. Decoding reverses the mask with the received nonce and compares the constant. A random
UUID passes with probability 2^-96; a real account UUID never does.

A deployment can replace the 80-bit constant: supply the same 10 bytes on both ends —
`HolepunchOptions.Builder.markerConstant` on the client and the matching
`NettyLoginClaimHandler.install` overload on the server — and only that deployment's clients are
claimed by its servers; a stock client and a customized server replay each other's logins to
vanilla. The constant is a routing key, not a secret: it never authenticates anything, and the
marker UUID itself is always built by the library, because the claim step must be able to decode
it.

### The transport handoff

The handoff is optional. A connection that never calls `commitTransportUpgrade()` keeps the
vanilla AES/CFB8 transport for its whole life: `onRead` and `write` keep working, nothing times
out, and the vanilla AES does all of the encryption work. It is a supported resting state, not a
phase you must pass through.

Bulk payloads belong past the flip: commit **before** large transfers and run them over the raw
stream (or TLS on top of it). The encrypted transport is bounded by Minecraft's serial AES/CFB8
cipher at roughly 60 MB/s per core, while the raw stream moves at memory speed; no amount of
copy-tuning on the encrypted path changes that ceiling, so treat ~60 MB/s per core as its design
limit and keep it for small protocol messages.

An application that wants to change the wire format midstream (AutoModpack inserts TLS) must not
race its own bytes into the old format. `commitTransportUpgrade()` exchanges an encrypted COMMIT
control frame in each direction, and both endpoints flip to the raw stream at the same stream
position. Bytes flushed before the local COMMIT travel the AES transport and surface in the
peer's `onRead`; writes submitted while the commit is in flight fail, so an application holds its
own output across the flip; writes after it travel the raw stream and surface in `onRawRead`.
Past the flip, mcholepunch does not interpret the bytes and does not authenticate the peer; the
stream is yours.

### Talking to a server without mcholepunch

The client expects the world it dials to speak the dialect above, and fails fast when it does
not. A vanilla server in online mode sends an Encryption Request with `shouldAuthenticate=true`;
mcholepunch never fakes Mojang authentication, so the connect future fails with
`AUTHENTICATION_REQUESTED`. A vanilla server in offline mode sends some other dialect instead
(Set Compression, Login Plugin Requests, Login Success), which surfaces as `UNEXPECTED_PACKET`.
A silent or filtered peer ends in `TIMEOUT` after the negotiation timeout. In every case the
connect future completes exceptionally and the socket closes; nothing hangs and nothing is sent
beyond the two vanilla-shaped login packets.

## Modules

| Module | Description |
|---|---|
| `:core` | The login codec (`LoginCodec`), the marker, the NIO client and its negotiation, the vanilla-style crypto, SRV resolution, and the transport handoff. No Netty. |
| `:server-netty` | The Netty server side: `NettyLoginClaimHandler`, the login negotiator that owns the taken-over channel, the vanilla-shaped framing handlers, and the raw connection plumbing. Depends on Netty 4.1 and `:core`. |

Package `pl.skidam.mcholepunch` holds the public API (`HolepunchClient`, `HolepunchConnection`,
`HolepunchHandler`, `HolepunchOptions`, `HolepunchRoute`, `HolepunchFailure`, and the `server.netty`
integration points: `NettyChannelRegistry`, `NettyLoginClaimHandler`, `HolepunchChannelApplication`).
These names are the stable surface. The rest of the public classes in `server.netty` are the
takeover machinery itself, and everything under `internal`, can all change without notice.

Tuning lives in `HolepunchOptions`: username (a random one is generated when absent), the Forge
and NeoForge `\0FML3\0` host suffix, connect and negotiation timeouts (10 s each), the executor
that runs client callbacks (ForkJoinPool common pool by default), and the send-side frame cap
(`maximumFrameBytes`, 256 KiB by default). The frame ceiling is the vanilla 2 MiB login cap;
writes above the send cap are split and the peer accepts up to the ceiling. AutoModpack streams
4 MiB contract body chunks, but TLS turns them into ~16 KiB records before they reach the
transport, so the 256 KiB default fits it without configuration. The client imposes no
pending-write cap: the stage returned by `write` completes only once the bytes reached the socket,
so apply backpressure by waiting on it before queueing more. The server's pending-write budget and
frame cap are the values passed to `NettyChannelRegistry.register` (the two-argument overload
keeps the 256 KiB frame default); Netty applies the pending budget as a write-buffer water mark
(`isWritable()`), not a hard cap.

## What it is not

There is no rendezvous server and no UDP punching here; the caller brings a reachable address,
whether that is a plain host or the result of its own holepunch coordination. There is no
gameplay takeover: a claimed login never becomes a player session. And there is no Mojang session
validation: the encryption exchange proves possession of the server keypair's counterparty, not
identity.

## Building and debugging

```sh
./gradlew build          # compile, run the JUnit 5 tests, jar both modules
./gradlew formatApply    # spotless, Eclipse formatter
```

Both modules target Java 17. Transport tracing, on the client and the server alike, is behind
`-Dmcholepunch.debug=true` (frame previews sized by `-Dmcholepunch.debug.previewBytes`, 16 by
default). Without that flag the library logs nothing on paths that return failures to their
caller: client outcomes come back through failed futures and the closed callback, server outcomes
after the handover through the channel and the commit future. Two things still print as a warning
because no caller exists to receive them: a server-side IO failure before the application is
installed, and an inbound pipeline exception an application handler did not handle. A malformed
peer frame is the peer's doing and records under tracing only.

## License

BSD 3-Clause, see `LICENSE`.
