# mcholepunch

A version-independent Java 17 library for running a custom byte stream over a Minecraft server's existing TCP port.

A client opens a connection that looks like a vanilla Minecraft login. A server-side Netty handler inspects the login before vanilla creates a login listener:

- **Unmarked logins** are replayed untouched so normal players join as usual.
- **Marked logins** are claimed, complete a vanilla-shaped RSA/AES-CFB8 key exchange using the server's own keypair, and hand off an encrypted byte stream (with an optional zero-copy upgrade to a raw stream) to your application.

There is no Minecraft code, no mixins, and no version tracking in this repository. You pass the target server's vanilla protocol number for the handshake; everything else on the wire is identical across Minecraft versions.

## Modules

| Module | Artifact | Description |
|---|---|---|
| `:core` | `mcholepunch-core` | NIO client, login codec, marker UUID codec, Minecraft RSA/AES-CFB8 crypto, RFC 2782 SRV resolver, and transport handoff state machine. Zero Netty dependency. |
| `:server-netty` | `mcholepunch-server-netty` | Netty 4.1 server integration: login claim handler, login negotiator, vanilla framing codecs, and taken-over channel transport. |

### Public API Surface

- **Core (`pl.skidam.mcholepunch`)**: `HolepunchClient`, `HolepunchConnection`, `HolepunchHandler`, `HolepunchOptions`, `HolepunchRoute`, `HolepunchFailure`
- **Server (`pl.skidam.mcholepunch.server.netty`)**: `NettyChannelRegistry`, `NettyLoginClaimHandler`, `HolepunchChannelApplication`

Everything under `pl.skidam.mcholepunch.internal` and the remaining plumbing classes in `server.netty` are internal implementation details.

## Quick Start

### Server (Netty)

Register your application once at startup, then install `NettyLoginClaimHandler` on every accepted Minecraft channel after vanilla's `"splitter"` frame decoder:

```java
// 1. Register the handler for claimed connections (once at startup)
NettyChannelRegistry.Registration registration = NettyChannelRegistry.register(
		1024 * 1024, // write-buffer water mark in bytes (channel.isWritable())
		(channel, connection) -> {
			channel.pipeline().addLast("app", new MyProtocolHandler(connection));
			connection.write(ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8)));
		}
);

// 2. Install on each accepted Minecraft server channel before vanilla owns the login
NettyLoginClaimHandler.install(channel.pipeline(), serverKeyPair);
```

### Client (NIO)

Resolve a server address (including `_minecraft._tcp` SRV records and IPv6 literals) or construct a `HolepunchRoute` directly when you already have the socket address:

```java
HolepunchRoute route = HolepunchClient.resolve("mc.example.com", 25565)
		.toCompletableFuture()
		.get();

// Or skip DNS when the transport address is already known:
// HolepunchRoute route = HolepunchRoute.at("mc.example.com", 25565, socketAddress);

HolepunchOptions options = HolepunchOptions.builder()
		.handshakeHostSuffix(HolepunchOptions.FORGE_FML3_HANDSHAKE_HOST_SUFFIX)
		.build();

HolepunchConnection connection = HolepunchClient.connect(route, 763, handler, options)
		.toCompletableFuture()
		.get(20, TimeUnit.SECONDS);
```

## Protocol Flow

1. **Connect**: The client resolves the target (applying RFC 2782 SRV priority and weight selection on port `25565`) and opens a TCP socket.
2. **Handshake + Login Start**: The client sends a vanilla Handshake (`packet 0`, login intention `2`) with the caller-supplied protocol version, followed by a Login Start (`packet 0`) containing a username and a generated marker UUID.
3. **Claim or Replay**: `NettyLoginClaimHandler` holds the Handshake frame (bounded by a 10s expiry timer) until Login Start arrives. If the UUID does not match or no application is registered, both frames are replayed downstream and the handler removes itself. If it matches, `NettyLoginNegotiator` strips the vanilla pipeline and takes over the channel.
4. **Encryption Exchange**: The server sends an Encryption Request (`packet 1`, `shouldAuthenticate = false`) with its RSA public key and a 4-byte challenge. The client generates a 16-byte AES secret, encrypts both secret and challenge with the server's RSA public key, replies with Encryption Response (`packet 1`), and both sides enable `AES/CFB8/NoPadding`.
5. **Takeover**: The server sends an encrypted Custom Query (`packet 4`, `mcholepunch:takeover`), swaps to the framed transport pipeline, and invokes `HolepunchChannelApplication.install(...)`. The client switches upon receiving the query and invokes `HolepunchHandler.onOpen(...)`.

## Marker UUIDs and Custom Routing

`HolepunchMarker` encodes 122 payload bits into an RFC 4122 v4-shaped UUID:

- 80-bit constant (default: first 10 bytes of `SHA-256("mcholepunch:marker-hash")`)
- 8-bit protocol version (`1`)
- 8-bit flags (`0`)
- 26-bit random nonce

The first 96 bits are XOR-masked with `SHA-256("mcholepunch:uuid-mask" || nonce)` so the constant never appears in plaintext on the wire. A random UUID has a `2^-96` chance of matching.

To isolate your deployment from other `mcholepunch` users, pass the same 10-byte constant on both ends:

- Client: `HolepunchOptions.builder().markerConstant(tenBytes)`
- Server: `NettyLoginClaimHandler.install(pipeline, serverKeyPair, tenBytes)`

The marker constant is a routing key, not a secret.

## Encrypted Transport vs. Raw Handoff

By default, connections run over a length-prefixed framed stream encrypted with Minecraft's vanilla `AES/CFB8`. This is a valid resting state for the full lifetime of a connection and works well for control traffic and small messages.

For bulk transfers, upgrade to the raw stream with `commitTransportUpgrade()` before sending large payloads:

- Vanilla `AES/CFB8` is serial and caps around ~60 MB/s per core, whereas the raw path passes buffers through without framing or cipher overhead.
- `commitTransportUpgrade()` exchanges an encrypted `COMMIT` control frame in both directions and flips both endpoints at the exact same stream position.
- Writes submitted while a commit is in flight fail immediately so application code does not race across the boundary.
- On the client, pre-handoff bytes arrive in `HolepunchHandler.onRead(ByteBuffer)` and post-handoff bytes arrive in `HolepunchHandler.onRawRead(ByteBuffer)` (which defaults to `onRead`).
- Both endpoints expose the shared 16-byte AES key via `connection.transportSecret()` if your layered protocol (such as TLS) wants to bind to it. Once raw mode is active, `mcholepunch` does not inspect or encrypt the stream.

## Tuning and Backpressure

| Setting | Default | Notes |
|---|---|---|
| `connectTimeout` | `10s` | TCP connect timeout on `HolepunchOptions`. |
| `negotiationTimeout` | `10s` | Total budget for encryption + takeover reads. |
| `callbackExecutor` | `ForkJoinPool.commonPool()` | Executor for client `HolepunchHandler` callbacks (serialized per connection). |
| `maximumFrameBytes` | `256 KiB` (`HolepunchOptions.DEFAULT_MAXIMUM_FRAME_BYTES`) | Send-side split threshold on client options or 3-arg `NettyChannelRegistry.register`. Receivers always accept up to `HolepunchOptions.MAX_FRAME_BYTES` (2 MiB). |
| `maximumPendingWriteBytes` | Caller-supplied (server) | Applied as Netty's `WriteBufferWaterMark` (`channel.isWritable()`), not a hard cap. |

On the client, `connection.write(ByteBuffer)` completes only once the bytes have been written to the socket; wait on the returned `CompletionStage` to apply backpressure, and use `pauseReads()` / `resumeReads()` for inbound flow control.

## Failure Modes

Client failures surface as `HolepunchFailure` (a subclass of `IOException`) with a typed `HolepunchFailure.Kind`:

- `AUTHENTICATION_REQUESTED`: Peer sent an Encryption Request with `shouldAuthenticate = true` (e.g., an online-mode vanilla server without `mcholepunch`).
- `UNEXPECTED_PACKET`: Peer sent an unrecognized packet during negotiation or takeover (e.g., an offline-mode vanilla server).
- `TIMEOUT`: Negotiation or takeover exceeded `negotiationTimeout`.
- `CLOSED`: Connection closed locally or by the peer.
- `IO`: Socket, cipher, or callback error.

## Logging and Tracing

Without debug flags, the library logs nothing on paths where errors are returned to callers. Only unreturnable server-side errors (an IO failure before application install, or an uncaught inbound pipeline exception) emit a single Log4j warning under the `"mcholepunch"` logger.

Enable verbose transport tracing with JVM system properties:

```sh
-Dmcholepunch.debug=true
-Dmcholepunch.debug.previewBytes=16      # optional, hex preview length (default 16)
-Dmcholepunch.debug.watchdogSeconds=5    # optional, periodic state dump interval (default 5)
```

## Non-Goals

- **No NAT traversal or rendezvous**: Callers provide a reachable address.
- **No gameplay takeover**: Claimed logins never become Minecraft player sessions.
- **No Mojang authentication**: The RSA/AES exchange binds to the server's keypair only.

## Building

Requires Java 17:

```sh
./gradlew build          # compile (-Xlint:all,-serial -Werror), test, spotlessCheck, jar
./gradlew formatApply    # apply Eclipse Java and ktlint Gradle formatting
```

## License

BSD 3-Clause. See [LICENSE](LICENSE).
