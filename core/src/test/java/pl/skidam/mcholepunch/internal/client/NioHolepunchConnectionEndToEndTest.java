package pl.skidam.mcholepunch.internal.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import pl.skidam.mcholepunch.HolepunchConnection;
import pl.skidam.mcholepunch.HolepunchFailure;
import pl.skidam.mcholepunch.HolepunchHandler;
import pl.skidam.mcholepunch.HolepunchOptions;
import pl.skidam.mcholepunch.HolepunchRoute;
import pl.skidam.mcholepunch.internal.HolepunchRequest;
import pl.skidam.mcholepunch.internal.crypto.MinecraftCrypt;
import pl.skidam.mcholepunch.internal.crypto.TransportEncryption;
import pl.skidam.mcholepunch.internal.net.VanillaAddressResolver;
import pl.skidam.mcholepunch.internal.protocol.HolepunchMarker;
import pl.skidam.mcholepunch.internal.protocol.LoginCodec;
import pl.skidam.mcholepunch.internal.protocol.MinecraftFrameReader;
import pl.skidam.mcholepunch.internal.protocol.MinecraftFrameWriter;
import pl.skidam.mcholepunch.internal.protocol.TransportFrame;
import pl.skidam.mcholepunch.internal.protocol.TransportFrameCodec;
import pl.skidam.mcholepunch.internal.protocol.TransportFrameReader;
import pl.skidam.mcholepunch.internal.transport.TransportFeed;
import pl.skidam.mcholepunch.internal.transport.TransportHandoff;

/** Drives the whole client conversation against an in-test server speaking the same wire protocol. */
class NioHolepunchConnectionEndToEndTest {
	private static final int TEST_TIMEOUT_SECONDS = 15;

	@Test
	void negotiatesEncryptsHandsOffAndSwitchesToRaw() throws Exception {
		List<String> events = Collections.synchronizedList(new ArrayList<>());
		CountDownLatch readLatch = new CountDownLatch(1);
		CountDownLatch rawReadLatch = new CountDownLatch(1);
		CountDownLatch closedLatch = new CountDownLatch(1);
		HolepunchHandler handler = new HolepunchHandler() {
			@Override
			public void onOpen(HolepunchConnection connection) {
				events.add("open");
			}

			@Override
			public void onRead(ByteBuffer data) {
				events.add("read:" + text(data));
				readLatch.countDown();
			}

			@Override
			public void onRawRead(ByteBuffer data) {
				events.add("raw:" + text(data));
				rawReadLatch.countDown();
			}

			@Override
			public void onClosed(HolepunchFailure failure) {
				events.add("closed");
				closedLatch.countDown();
			}
		};

		try (TestServer server = new TestServer()) {
			server.start();
			HolepunchRoute route = server.route();
			HolepunchRequest request = new HolepunchRequest(route, 763, handler, HolepunchOptions.builder().build());
			NioHolepunchClient client = new NioHolepunchClient(new VanillaAddressResolver() {
				@Override
				public HolepunchRoute resolve(String host, int port) {
					return route;
				}
			});

			HolepunchConnection connection = client.connect(request).toCompletableFuture().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			assertEquals(16, connection.transportSecret().length, "the negotiated transport secret must reach the application");

			server.writeTransportData(ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8)));
			assertTrue(readLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
			assertEquals(List.of("open", "read:hello"), List.copyOf(events).subList(0, 2), "onOpen must run before any read");

			connection.write(ByteBuffer.wrap("ping".getBytes(StandardCharsets.UTF_8))).toCompletableFuture().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			assertTrue(server.awaitClientData("ping"));

			CompletionStage<Void> committed = connection.commitTransportUpgrade();
			CompletionStage<Void> rejected = connection.write(ByteBuffer.wrap("dropped".getBytes(StandardCharsets.UTF_8)));
			assertTrue(rejected.toCompletableFuture().isCompletedExceptionally(), "writes during the commit flight must fail");

			server.commit();
			committed.toCompletableFuture().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			assertTrue(connection.isRaw());

			connection.write(ByteBuffer.wrap("raw-out".getBytes(StandardCharsets.UTF_8))).toCompletableFuture().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			assertTrue(server.awaitRawClientData("raw-out"));
			server.writeRaw("raw-in".getBytes(StandardCharsets.UTF_8));
			assertTrue(rawReadLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));

			assertNull(server.failure());
			assertEquals(List.of("open", "read:hello"), List.copyOf(events).subList(0, 2));
			assertTrue(events.contains("raw:raw-in"));

			connection.close();
			assertTrue(closedLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
			assertTrue(events.contains("closed"));
		}
	}

	/** A silent server must end the connect future at the negotiation deadline, not block the read forever. */
	@Test
	void silentServerTimesOutAtTheNegotiationDeadline() throws Exception {
		ServerSocketChannel listener = ServerSocketChannel.open();
		listener.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
		SocketChannel[] peer = new SocketChannel[1];
		Thread acceptor = new Thread(() -> {
			try {
				peer[0] = listener.accept();
			} catch (IOException ignored) {
			}
		}, "mcholepunch-silent-server");
		acceptor.setDaemon(true);
		acceptor.start();
		try (listener) {
			HolepunchRoute route = HolepunchRoute.at("server.test", 25565, (InetSocketAddress) listener.socket().getLocalSocketAddress());
			HolepunchOptions options = HolepunchOptions.builder().connectTimeout(Duration.ofSeconds(5)).negotiationTimeout(Duration.ofMillis(500)).build();
			HolepunchRequest request = new HolepunchRequest(route, 763, new HolepunchHandler() {
			}, options);
			NioHolepunchClient client = new NioHolepunchClient(new VanillaAddressResolver() {
				@Override
				public HolepunchRoute resolve(String host, int port) {
					return route;
				}
			});

			long startNanos = System.nanoTime();
			ExecutionException failure = assertThrows(ExecutionException.class, () -> client.connect(request).toCompletableFuture().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
			long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
			assertInstanceOf(HolepunchFailure.class, failure.getCause());
			assertEquals(HolepunchFailure.Kind.TIMEOUT, ((HolepunchFailure) failure.getCause()).kind());
			assertTrue(elapsedMillis < 4000, "the negotiation deadline must break the blocked read, took " + elapsedMillis + "ms");
		} finally {
			if (peer[0] != null) {
				peer[0].close();
			}
			acceptor.join(1000);
			listener.close();
		}
	}

	/** The README's install example writes right after the query, so the first bytes can share the query's TCP read. */
	@Test
	void deliversApplicationBytesCoalescedWithTheTakeoverQuery() throws Exception {
		List<String> events = Collections.synchronizedList(new ArrayList<>());
		CountDownLatch readLatch = new CountDownLatch(1);
		HolepunchHandler handler = new HolepunchHandler() {
			@Override
			public void onOpen(HolepunchConnection connection) {
				events.add("open");
			}

			@Override
			public void onRead(ByteBuffer data) {
				events.add("read:" + text(data));
				readLatch.countDown();
			}

			@Override
			public void onClosed(HolepunchFailure failure) {
				events.add("closed");
			}
		};

		try (TestServer server = new TestServer()) {
			server.writeInitialDataWithTakeoverQuery(ByteBuffer.wrap("early".getBytes(StandardCharsets.UTF_8)));
			server.start();
			HolepunchRoute route = server.route();
			HolepunchRequest request = new HolepunchRequest(route, 763, handler, HolepunchOptions.builder().build());
			NioHolepunchClient client = new NioHolepunchClient(new VanillaAddressResolver() {
				@Override
				public HolepunchRoute resolve(String host, int port) {
					return route;
				}
			});

			HolepunchConnection connection = client.connect(request).toCompletableFuture().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			assertTrue(readLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
			assertEquals(List.of("open", "read:early"), List.copyOf(events).subList(0, 2), "bytes coalesced with the query must surface as initial data after onOpen");

			connection.write(ByteBuffer.wrap("ping".getBytes(StandardCharsets.UTF_8))).toCompletableFuture().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			assertTrue(server.awaitClientData("ping"), "the connection must keep working after a coalesced first read");
			assertNull(server.failure());
			connection.close();
		}
	}

	/**
	 * Writes racing the commit must never corrupt the stream: a write is either ordered before the
	 * COMMIT task on the writer (encrypted era) or rejected up front, so a corrupted framing would
	 * surface as an unknown frame type on the server's read loop.
	 */
	@Test
	void writesRacingCommitsKeepTheStreamIntact() throws Exception {
		try (TestServer server = new TestServer()) {
			server.start();
			HolepunchRoute route = server.route();
			HolepunchRequest request = new HolepunchRequest(route, 763, new HolepunchHandler() {
			}, HolepunchOptions.builder().build());
			HolepunchConnection connection = new NioHolepunchClient().connect(request).toCompletableFuture().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

			// Every write that succeeded is recorded, in order; the era bug this test guards sends a
			// transport-framed chunk through the raw path, and the raw reader accepts any bytes, so
			// the only visible marker is the frame prefix inside the raw stream. One write completes
			// before the commit request, so the encrypted era is always exercised.
			List<String> written = Collections.synchronizedList(new ArrayList<>());
			AtomicReference<Exception> hung = new AtomicReference<>();
			String before = "before";
			connection.write(ByteBuffer.wrap(before.getBytes(StandardCharsets.UTF_8))).toCompletableFuture().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			written.add(before);
			CountDownLatch writesDone = new CountDownLatch(1);
			Thread writer = new Thread(() -> {
				for (int index = 0; index < 200; index++) {
					String payload = "w" + index;
					try {
						connection.write(ByteBuffer.wrap(payload.getBytes(StandardCharsets.UTF_8))).toCompletableFuture().get(5, TimeUnit.SECONDS);
						written.add(payload);
					} catch (InterruptedException interrupted) {
						Thread.currentThread().interrupt();
						hung.compareAndSet(null, interrupted);
						writesDone.countDown();
						return;
					} catch (TimeoutException timedOut) {
						hung.compareAndSet(null, timedOut);
						writesDone.countDown();
						return;
					} catch (ExecutionException rejected) {
						// Writes that land in the commit window fail by contract; the loop continues.
						// A hung write surfaces as TimeoutException and fails the test.
					}
				}
				writesDone.countDown();
			}, "mcholepunch-racing-writer");
			writer.start();
			server.commit();
			CompletionStage<Void> commit = connection.commitTransportUpgrade();
			commit.toCompletableFuture().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			assertTrue(writesDone.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
			writer.join(1000);
			assertNull(hung.get(), "a racing write hung: " + hung.get());
			assertNull(server.failure(), "the stream must not corrupt while writes race the commit");

			// A write future completes when the bytes reach the socket, not when the server's read
			// loop has processed them, so poll for the full length before asserting.
			String expected = String.join("", written);
			String delivered = server.awaitClientAndRawDataJoined(expected.length());
			// The pre-commit write pins the encrypted era: a regression that sends it raw, unframed,
			// would still satisfy the combined assertion above.
			assertTrue(server.clientDataJoined().startsWith(before), "the pre-commit write must arrive through the encrypted era");
			assertFalse(server.rawClientDataJoined().contains(before), "the pre-commit write must not leak into the raw era");
			assertFalse(server.rawClientDataJoined().indexOf('\u0000') != -1, "the raw era must not carry a transport frame prefix (0x00 is absent from every payload)");
			assertEquals(expected, delivered, "every successful write must reach exactly one era, whole and in order");

			connection.write(ByteBuffer.wrap("after".getBytes(StandardCharsets.UTF_8))).toCompletableFuture().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			assertTrue(server.awaitRawClientData("after"), "the raw era must still carry post-commit writes");
			connection.close();
		}
	}

	/** A write that races a close must always complete: close() used to discard enqueued tasks with their futures unowned. */
	@Test
	void writesRacingClosesAlwaysComplete() throws Exception {
		for (int attempt = 0; attempt < 25; attempt++) {
			try (TestServer server = new TestServer()) {
				server.start();
				HolepunchRoute route = server.route();
				HolepunchRequest request = new HolepunchRequest(route, 763, new HolepunchHandler() {
				}, HolepunchOptions.builder().build());
				HolepunchConnection connection = new NioHolepunchClient().connect(request).toCompletableFuture().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

				List<CompletionStage<Void>> writes = new ArrayList<>();
				for (int index = 0; index < 20; index++) {
					writes.add(connection.write(ByteBuffer.wrap(("w" + index).getBytes(StandardCharsets.UTF_8))));
				}
				connection.close();
				for (CompletionStage<Void> write : writes) {
					try {
						write.toCompletableFuture().get(5, TimeUnit.SECONDS);
					} catch (ExecutionException expected) {
						// Bytes past the close fail; what must never happen is a hang.
					}
				}
			}
		}
	}

	/**
	 * A commit that races a close must always complete: fail() used to shut the writer down before
	 * the commit future was stored, leaving a committer waiting forever. The interleaving is a
	 * narrow window, so this hammers it instead of pinning the exact timing.
	 */
	@Test
	void commitsRacingClosesAlwaysComplete() throws Exception {
		for (int attempt = 0; attempt < 25; attempt++) {
			try (TestServer server = new TestServer()) {
				server.start();
				HolepunchRoute route = server.route();
				HolepunchRequest request = new HolepunchRequest(route, 763, new HolepunchHandler() {
				}, HolepunchOptions.builder().build());
				HolepunchConnection connection = new NioHolepunchClient().connect(request).toCompletableFuture().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

				CompletionStage<Void> commit = connection.commitTransportUpgrade();
				connection.close();
				try {
					commit.toCompletableFuture().get(5, TimeUnit.SECONDS);
				} catch (ExecutionException expected) {
					// Closing during the commit flight fails the commit; what must never happen is a hang.
				}
			}
		}
	}

	private static String text(ByteBuffer data) {
		byte[] bytes = new byte[data.remaining()];
		data.duplicate().get(bytes);
		return new String(bytes, StandardCharsets.UTF_8);
	}

	/** The server half of the protocol: vanilla-shaped login, RSA/AES exchange, takeover query, handoff, raw stream. */
	private static final class TestServer implements AutoCloseable {
		private final ServerSocketChannel listener;
		private final KeyPair keyPair;
		private final byte[] challenge = MinecraftCrypt.randomBytes(4);
		private final List<String> clientData = Collections.synchronizedList(new ArrayList<>());
		private final List<String> rawClientData = Collections.synchronizedList(new ArrayList<>());
		private final CountDownLatch clientDataLatch = new CountDownLatch(1);
		private final CountDownLatch rawClientDataLatch = new CountDownLatch(1);
		private final CountDownLatch running = new CountDownLatch(1);
		private final ArrayDeque<ByteBuffer> loginFrames = new ArrayDeque<>();
		private volatile IOException failure;
		private volatile ByteBuffer initialDataWithQuery;
		private SocketChannel channel;
		private TransportEncryption encryption;
		private TransportHandoff handoff;

		TestServer() throws Exception {
			listener = ServerSocketChannel.open();
			listener.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
			KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
			generator.initialize(512);
			keyPair = generator.generateKeyPair();
		}

		HolepunchRoute route() {
			return HolepunchRoute.at("server.test", 25565, (InetSocketAddress) listener.socket().getLocalSocketAddress());
		}

		void start() {
			Thread thread = new Thread(this::serve, "mcholepunch-test-server");
			thread.setDaemon(true);
			thread.start();
		}

		/** Appends the first application data frame to the takeover query's TCP write; call before {@link #start()}. */
		void writeInitialDataWithTakeoverQuery(ByteBuffer payload) {
			initialDataWithQuery = payload;
		}

		private void serve() {
			try {
				listener.socket().setSoTimeout(TEST_TIMEOUT_SECONDS * 1000);
				channel = listener.accept();
				channel.socket().setSoTimeout(TEST_TIMEOUT_SECONDS * 1000);
				negotiate(channel);
				running.countDown();
				readLoop(channel);
			} catch (Exception exception) {
				if (!(exception instanceof SocketTimeoutException)) {
					failure = exception instanceof IOException ioException ? ioException : new IOException(exception);
				}
			}
		}

		private void negotiate(SocketChannel channel) throws Exception {
			MinecraftFrameReader loginReader = new MinecraftFrameReader(LoginCodec.MAX_FRAME_BYTES);
			ByteBuffer handshake = nextLoginFrame(channel, loginReader);
			assertTrue(LoginCodec.isLoginIntention(handshake));
			ByteBuffer loginStart = nextLoginFrame(channel, loginReader);
			assertTrue(HolepunchMarker.decode(LoginCodec.readLoginStart(loginStart).profileId()).isPresent());

			ByteBuffer request = LoginCodec.encryptionRequestPacket(keyPair.getPublic().getEncoded(), challenge, false);
			byte[] requestBytes = new byte[request.remaining()];
			request.get(requestBytes);
			writePlain(channel, MinecraftFrameWriter.frame(requestBytes));
			LoginCodec.EncryptionResponse response = LoginCodec.readEncryptionResponse(nextLoginFrame(channel, loginReader));
			byte[] secret = MinecraftCrypt.decryptLoginSecret(keyPair.getPrivate(), response.encryptedSecret(), response.encryptedChallenge(), challenge);
			encryption = TransportEncryption.server(secret);
			handoff = new TransportHandoff();

			ByteBuffer query = LoginCodec.takeoverQueryPacket(0);
			byte[] queryBytes = new byte[query.remaining()];
			query.get(queryBytes);
			ByteBuffer initialData = initialDataWithQuery;
			if (initialData == null) {
				writeEncrypted(channel, MinecraftFrameWriter.frame(queryBytes));
			} else {
				// One TCP write carrying the takeover query and the first data frame, the way a server
				// that greets the client right after the query produces.
				ByteBuffer queryFrame = MinecraftFrameWriter.frame(queryBytes);
				ByteBuffer combined = ByteBuffer.allocate(queryFrame.remaining() + TransportFrameCodec.data(initialData.duplicate()).remaining());
				combined.put(queryFrame);
				combined.put(TransportFrameCodec.data(initialData));
				combined.flip();
				writeEncrypted(channel, combined);
			}
		}

		/** Reads one Minecraft-framed login packet, tolerating coalesced frames across TCP reads. */
		private ByteBuffer nextLoginFrame(SocketChannel channel, MinecraftFrameReader reader) throws IOException {
			ByteBuffer frame = loginFrames.poll();
			ByteBuffer network = ByteBuffer.allocate(8192);
			while (frame == null) {
				network.clear();
				int read = channel.read(network);
				if (read < 0) {
					throw new IOException("client closed during negotiation");
				}
				network.flip();
				loginFrames.addAll(reader.feed(network));
				frame = loginFrames.poll();
			}
			return frame;
		}

		private void readLoop(SocketChannel channel) throws Exception {
			TransportFeed feed = new TransportFeed(encryption, new TransportFrameReader(TransportFrame.MAX_FRAME_BYTES));
			ByteBuffer network = ByteBuffer.allocate(8192);
			while (true) {
				network.clear();
				int read = channel.read(network);
				if (read < 0) {
					return;
				}
				network.flip();
				while (network.hasRemaining()) {
					if (feed.isRaw()) {
						ByteBuffer raw = network.slice();
						network.position(network.limit());
						byte[] bytes = new byte[raw.remaining()];
						raw.get(bytes);
						rawClientData.add(new String(bytes, StandardCharsets.UTF_8));
						rawClientDataLatch.countDown();
						continue;
					}
					feed.feed(network, frame -> {
						switch (frame.type()) {
							case DATA -> {
								clientData.add(new String(frame.payload(), StandardCharsets.UTF_8));
								clientDataLatch.countDown();
							}
							case COMMIT -> {
								if (handoff.onPeerCommitReceived()) {
									encryption.activateRaw();
								}
							}
						}
					});
				}
			}
		}

		private synchronized void writePlain(SocketChannel channel, ByteBuffer data) throws IOException {
			while (data.hasRemaining()) {
				channel.write(data);
			}
		}

		private synchronized void writeEncrypted(SocketChannel channel, ByteBuffer plaintext) throws IOException {
			ByteBuffer output = ByteBuffer.allocate(plaintext.remaining());
			encryption.encrypt(plaintext, output);
			output.flip();
			while (output.hasRemaining()) {
				channel.write(output);
			}
		}

		synchronized void writeTransportData(ByteBuffer payload) throws IOException {
			awaitServerReady();
			writeEncrypted(channel, TransportFrameCodec.data(payload));
		}

		synchronized void commit() throws IOException {
			awaitServerReady();
			if (handoff.requestCommit()) {
				writeEncrypted(channel, TransportFrameCodec.commit());
				if (handoff.onCommitSent()) {
					encryption.activateRaw();
				}
			}
		}

		synchronized void writeRaw(byte[] bytes) throws IOException {
			awaitServerReady();
			ByteBuffer buffer = ByteBuffer.wrap(bytes);
			while (buffer.hasRemaining()) {
				channel.write(buffer);
			}
		}

		private void awaitServerReady() {
			try {
				assertTrue(running.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
			} catch (InterruptedException exception) {
				Thread.currentThread().interrupt();
			}
		}

		String clientDataJoined() {
			synchronized (clientData) {
				return String.join("", clientData);
			}
		}

		String rawClientDataJoined() {
			synchronized (rawClientData) {
				return String.join("", rawClientData);
			}
		}

		/** Waits until both eras together carry the expected number of bytes; a write future completes before the server reads them. */
		String awaitClientAndRawDataJoined(int expectedLength) throws InterruptedException {
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TEST_TIMEOUT_SECONDS);
			String joined = "";
			while (System.nanoTime() < deadline) {
				joined = clientDataJoined() + rawClientDataJoined();
				if (joined.length() >= expectedLength) {
					return joined;
				}
				Thread.sleep(10);
			}
			return joined;
		}

		boolean awaitClientData(String expected) throws InterruptedException {
			return awaitValue(clientData, clientDataLatch, expected);
		}

		boolean awaitRawClientData(String expected) throws InterruptedException {
			return awaitValue(rawClientData, rawClientDataLatch, expected);
		}

		private boolean awaitValue(List<String> values, CountDownLatch latch, String expected) throws InterruptedException {
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TEST_TIMEOUT_SECONDS);
			while (System.nanoTime() < deadline) {
				if (values.contains(expected)) {
					return true;
				}
				latch.await(100, TimeUnit.MILLISECONDS);
			}
			return values.contains(expected);
		}

		IOException failure() {
			return failure;
		}

		@Override
		public void close() throws IOException {
			listener.close();
			if (channel != null) {
				channel.close();
			}
		}
	}
}
