package pl.skidam.mcholepunch.internal.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import pl.skidam.mcholepunch.HolepunchConnection;
import pl.skidam.mcholepunch.HolepunchFailure;
import pl.skidam.mcholepunch.HolepunchHandler;
import pl.skidam.mcholepunch.HolepunchOptions;
import pl.skidam.mcholepunch.HolepunchRequest;
import pl.skidam.mcholepunch.HolepunchRoute;
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
			HolepunchRequest request = new HolepunchRequest(route.host(), route.port(), 763, handler, HolepunchOptions.builder().build());
			NioHolepunchClient client = new NioHolepunchClient(new VanillaAddressResolver() {
				@Override
				public HolepunchRoute resolve(String host, int port) {
					return route;
				}
			});

			HolepunchConnection connection = client.connect(request, route).toCompletableFuture().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

			server.writeTransportData(ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8)));
			assertTrue(readLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
			assertEquals(List.of("open", "read:hello"), List.copyOf(events).subList(0, 2), "onOpen must run before any read");

			connection.write(ByteBuffer.wrap("ping".getBytes(StandardCharsets.UTF_8))).toCompletableFuture().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			assertTrue(server.awaitClientData("ping"));

			CompletableFuture<Void> prepared = connection.prepareTransportUpgrade().toCompletableFuture();
			server.prepare();
			prepared.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			assertTrue(server.awaitPrepared());
			CompletionStage<Void> rejected = connection.write(ByteBuffer.wrap("dropped".getBytes(StandardCharsets.UTF_8)));
			assertTrue(rejected.toCompletableFuture().isCompletedExceptionally(), "writes during the upgrade window must fail");

			server.commit();
			connection.commitTransportUpgrade().toCompletableFuture().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
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
		private final CountDownLatch preparedLatch = new CountDownLatch(1);
		private final CountDownLatch running = new CountDownLatch(1);
		private final ArrayDeque<ByteBuffer> loginFrames = new ArrayDeque<>();
		private volatile IOException failure;
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
			return new HolepunchRoute("server.test", 25565, (InetSocketAddress) listener.socket().getLocalSocketAddress());
		}

		void start() {
			Thread thread = new Thread(this::serve, "mcholepunch-test-server");
			thread.setDaemon(true);
			thread.start();
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
			writeEncrypted(channel, MinecraftFrameWriter.frame(queryBytes));
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
					for (TransportFrame frame : feed.feed(network)) {
						switch (frame.type()) {
							case DATA -> {
								clientData.add(new String(frame.payload(), StandardCharsets.UTF_8));
								clientDataLatch.countDown();
							}
							case READY, ACK, COMMIT -> sendOutbound(channel, apply(handoff.onPeerControlReceived(frame.type())));
						}
					}
					if (handoff.isPrepared()) {
						preparedLatch.countDown();
					}
				}
			}
		}

		private TransportHandoff.Transition apply(TransportHandoff.Transition transition) {
			if (transition.activatesRaw()) {
				encryption.activateRaw();
			}
			return transition;
		}

		private void sendOutbound(SocketChannel channel, TransportHandoff.Transition transition) throws IOException {
			for (TransportFrame.Type type : transition.outbound()) {
				writeEncrypted(channel, switch (type) {
					case READY -> TransportFrameCodec.ready();
					case ACK -> TransportFrameCodec.ack();
					case COMMIT -> TransportFrameCodec.commit();
					case DATA -> throw new IllegalArgumentException("controls only");
				});
				sendOutbound(channel, apply(handoff.onLocalControlSent(type)));
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

		synchronized void prepare() throws IOException {
			awaitServerReady();
			sendOutbound(channel, apply(handoff.requestPrepare()));
		}

		synchronized void commit() throws IOException {
			awaitServerReady();
			sendOutbound(channel, apply(handoff.requestCommit()));
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

		boolean awaitPrepared() throws InterruptedException {
			return preparedLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
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
