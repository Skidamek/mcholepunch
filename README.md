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
HolepunchServerRegistry.register(executor, (username, address, marker) ->
		new HolepunchHandler() {
			@Override
			public void onOpen(HolepunchConnection connection) {
				connection.write(ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8)));
			}

			@Override
			public void onRead(ByteBuffer data) { /* application bytes, in order */ }

			@Override
			public void onClosed(HolepunchFailure failure) { /* why it ended */ }
		});

// server, in the Netty pipeline of every accepted Minecraft channel,
// added after the "splitter" handler, before vanilla can own the login:
channel.pipeline().addAfter("splitter", "mcholepunch", new NettyLoginClaimHandler(serverKeyPair));

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

## How it works

1. The client resolves the address the way Minecraft does (SRV records, IPv6 literals) and opens a
   TCP connection to it.
2. It sends a Handshake (packet 0, login intention) carrying the protocol number you supplied, then
   a Login Start (packet 0). On every version the Login Start is `name + UUID`, the newest shape,
   because the marker hides in the UUID and no vanilla decoder ever sees the packet.
3. `NettyLoginClaimHandler` holds the handshake frame, reads the Login Start, and checks the UUID.
   No marker: it replays the original bytes downstream and removes itself, and the login proceeds
   exactly as vanilla would have run it. Marker: it removes vanilla's future ownership before a
   login listener exists.
4. The server sends an Encryption Request (packet 1, `shouldAuthenticate` always false). The
   client answers with a fresh AES secret sealed to the server's RSA public key, and both sides
   switch to AES/CFB8.
5. The server sends an encrypted Custom Query (packet 4), `mcholepunch:takeover`, strips the
   vanilla handlers listed in `NettyTakeoverSpec.minecraftLogin()`, and from that point the
   connection delivers `ByteBuffer`s to the registered `HolepunchHandler`.

### The marker

A marker UUID is built by `HolepunchMarker`: 122 payload bits packed into an RFC 4122 v4-shaped
UUID. The payload is an 80-bit constant (SHA-256 of `mcholepunch:marker-hash`, truncated), a
version byte, a flags byte, and a 26-bit random nonce. The first 96 bits are XOR-masked with
SHA-256 of `mcholepunch:uuid-mask` keyed by the nonce, so the constant does not appear in the
clear. Decoding reverses the mask with the received nonce and compares the constant. A random
UUID passes with probability 2^-96; a real account UUID never does.

### The transport handoff

An application that wants to change the wire format midstream (AutoModpack inserts TLS) must not
race its own bytes into the old format. `commitTransportUpgrade()` exchanges an encrypted COMMIT
control frame in each direction, and both endpoints flip to the raw stream at the same stream
position. Bytes flushed before the local COMMIT travel the AES transport and surface in the
peer's `onRead`; writes submitted while the commit is in flight fail, so an application holds its
own output across the flip; writes after it travel the raw stream and surface in `onRawRead`.
Past the flip, mcholepunch does not interpret the bytes and does not authenticate the peer; the
stream is yours.

## Modules

| | |
|---|---|
| `:core` | The login codec (`LoginCodec`), the marker, the NIO client and its negotiation, the vanilla-style crypto, SRV resolution, the transport handoff, and `HolepunchServerRegistry`. No Netty. |
| `:server-netty` | The Netty server side: `NettyLoginClaimHandler`, the login negotiator, cipher handlers, the takeover that strips vanilla handlers, and the raw connection plumbing. Depends on Netty 4.1 and `:core`. |

Package `pl.skidam.mcholepunch` holds the public API (`HolepunchClient`, `HolepunchConnection`,
`HolepunchHandler`, `HolepunchOptions`, `HolepunchRoute`, `HolepunchFailure`,
`HolepunchServerRegistry`, and the `server.netty` handlers). Everything under `internal` can
change without notice.

Tuning lives in `HolepunchOptions`: username (a random one is generated when absent), the Forge
and NeoForge `\0FML3\0` host suffix, connect and negotiation timeouts (10 s each), the handler
setup timeout (5 s), the per-connection pending-write cap (4 MiB by default, shared by the client
and the server registry), and the executor that runs callbacks (ForkJoinPool common pool by
default). The server registry accepts an explicit pending-write cap in its longer `register`
overload when an application needs one sized to its own frames.

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

Both modules target Java 17. Server-side tracing is behind
`-Dmcholepunch.debug=true` (frame previews sized by `-Dmcholepunch.debug.previewBytes`, 16 by
default).

## License

BSD 3-Clause, see `LICENSE`.
