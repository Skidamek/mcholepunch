package pl.skidam.mcholepunch.server.netty;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import pl.skidam.mcholepunch.HolepunchConnection;
import pl.skidam.mcholepunch.HolepunchFailure;
import pl.skidam.mcholepunch.HolepunchHandler;
import pl.skidam.mcholepunch.HolepunchOptions;
import pl.skidam.mcholepunch.HolepunchRoute;
import pl.skidam.mcholepunch.internal.HolepunchRequest;
import pl.skidam.mcholepunch.internal.client.NioHolepunchClient;

/**
 * Runs the real NIO client against the real Netty claim, negotiation and transport pipeline over a
 * loopback socket, once on the built-in marker constant and once with matching custom constants.
 */
class HolepunchClientServerEndToEndTest {
	private static final int TEST_TIMEOUT_SECONDS = 15;
	private static final byte[] MARKER_CONSTANT = {(byte) 0xA5, 0x5A, (byte) 0xC3, 0x3C, (byte) 0x96, 0x69, (byte) 0x7E, (byte) 0x87, 0x11, (byte) 0xEE};

	private EventLoopGroup group;
	private Channel listener;
	private KeyPair keyPair;
	private NettyChannelRegistry.Registration registration;
	/** Null installs the claim handler on the built-in marker constant; the accept thread reads it. */
	private volatile byte[] markerConstant = MARKER_CONSTANT;
	private final List<String> serverReceived = Collections.synchronizedList(new ArrayList<>());
	private final List<String> clientReceived = new ArrayList<>();
	private final CountDownLatch serverOpen = new CountDownLatch(1);
	private final CountDownLatch clientOpen = new CountDownLatch(1);
	private final CountDownLatch clientRead = new CountDownLatch(1);
	private final CountDownLatch clientRawRead = new CountDownLatch(1);
	private final AtomicReference<HolepunchConnection> serverConnection = new AtomicReference<>();
	private volatile HolepunchConnection clientConnection;

	@BeforeEach
	void startServer() throws Exception {
		KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
		generator.initialize(512);
		keyPair = generator.generateKeyPair();
		group = new NioEventLoopGroup(1);
		listener = new ServerBootstrap().group(group, group).channel(NioServerSocketChannel.class).childHandler(new ChannelInitializer<SocketChannel>() {
			@Override
			protected void initChannel(SocketChannel channel) {
				// The claim handler anchors itself after the vanilla inbound frame decoder, named
				// "splitter" in production; our decoder stands in for it, so the claim handler sees
				// unframed login packets exactly as it does behind vanilla.
				channel.pipeline().addFirst("splitter", new VanillaFrameDecoder(64 * 1024));
				if (markerConstant == null) {
					NettyLoginClaimHandler.install(channel.pipeline(), keyPair);
				} else {
					NettyLoginClaimHandler.install(channel.pipeline(), keyPair, markerConstant);
				}
			}
		}).bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0)).sync().channel();
		registration = NettyChannelRegistry.register(1024 * 1024, (channel, connection) -> {
			serverConnection.set(connection);
			channel.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
				@Override
				protected void channelRead0(ChannelHandlerContext context, ByteBuf frame) {
					byte[] bytes = new byte[frame.readableBytes()];
					frame.readBytes(bytes);
					serverReceived.add(new String(bytes, StandardCharsets.UTF_8));
				}
			});
			serverOpen.countDown();
		});
	}

	@AfterEach
	void stopServer() throws Exception {
		if (registration != null) {
			registration.close();
		}
		if (listener != null) {
			listener.close().sync();
		}
		if (group != null) {
			group.shutdownGracefully(0, 2, TimeUnit.SECONDS).sync();
		}
	}

	@Test
	void negotiatesWithTheBuiltInMarkerConstant() throws Exception {
		runConversation(null);
	}

	@Test
	void negotiatesWithACustomMarkerConstantOnBothEnds() throws Exception {
		runConversation(MARKER_CONSTANT);
	}

	private void runConversation(byte[] conversationConstant) throws Exception {
		markerConstant = conversationConstant;
		HolepunchOptions.Builder options = HolepunchOptions.builder();
		if (conversationConstant != null) {
			options.markerConstant(conversationConstant);
		}
		HolepunchHandler handler = new HolepunchHandler() {
			@Override
			public void onOpen(HolepunchConnection connection) {
				clientConnection = connection;
				clientOpen.countDown();
			}

			@Override
			public void onRead(ByteBuffer data) {
				synchronized (clientReceived) {
					clientReceived.add(text(data));
				}
				clientRead.countDown();
			}

			@Override
			public void onRawRead(ByteBuffer data) {
				synchronized (clientReceived) {
					clientReceived.add("raw:" + text(data));
				}
				clientRawRead.countDown();
			}

			@Override
			public void onClosed(HolepunchFailure failure) {}
		};
		HolepunchRequest request = new HolepunchRequest(route(), 763, handler, options.build());
		NioHolepunchClient client = new NioHolepunchClient();

		HolepunchConnection connection = client.connect(request).toCompletableFuture().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
		assertTrue(serverOpen.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS), "the server application must receive the taken-over channel");
		assertTrue(clientOpen.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS), "onOpen must have run");
		assertNotNull(clientConnection);
		assertEquals(16, connection.transportSecret().length, "the negotiated transport secret must reach the application");
		assertArrayEquals(connection.transportSecret(), serverConnection.get().transportSecret(), "both ends must derive the same secret");

		serverConnection.get().write(ByteBuffer.wrap("hello client".getBytes(StandardCharsets.UTF_8)));
		assertTrue(awaitValue(clientReceived, clientRead, "hello client"), "the client must receive the server's transport write");

		connection.write(ByteBuffer.wrap("hello server".getBytes(StandardCharsets.UTF_8))).toCompletableFuture().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
		assertTrue(awaitServerValue("hello server"), "the server application must receive the client's transport write");

		// Above the 4 MB frame cap: the client must split it and the server must reassemble the
		// bytes. The earlier small exchange is cleared out so the count is the large write's alone.
		int oversized = 5 * 1024 * 1024 + 12345;
		synchronized (serverReceived) {
			serverReceived.clear();
		}
		connection.write(ByteBuffer.allocate(oversized)).toCompletableFuture().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
		assertEquals(oversized, awaitServerTotal(oversized), "the split oversized write must reach the server intact, byte for byte");

		HolepunchConnection serverSide = serverConnection.get();
		CompletionStage<Void> serverCommit = serverSide.commitTransportUpgrade();
		CompletionStage<Void> clientCommit = connection.commitTransportUpgrade();
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TEST_TIMEOUT_SECONDS);
		while (!(connection.isRaw() && serverSide.isRaw()) && System.nanoTime() < deadline) {
			Thread.sleep(10);
		}
		assertTrue(connection.isRaw() && serverSide.isRaw(), "both ends must reach the raw era");
		clientCommit.toCompletableFuture().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
		serverCommit.toCompletableFuture().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

		connection.write(ByteBuffer.wrap("raw client".getBytes(StandardCharsets.UTF_8))).toCompletableFuture().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
		assertTrue(awaitServerValue("raw client"), "the server must receive raw pass-through bytes");

		serverSide.write(ByteBuffer.wrap("raw server".getBytes(StandardCharsets.UTF_8)));
		assertTrue(awaitValue(clientReceived, clientRawRead, "raw:raw server"), "the client must receive the server's raw write");

		connection.close();
	}

	private HolepunchRoute route() {
		return HolepunchRoute.at("server.test", 25565, (InetSocketAddress) listener.localAddress());
	}

	private static String text(ByteBuffer data) {
		byte[] bytes = new byte[data.remaining()];
		data.duplicate().get(bytes);
		return new String(bytes, StandardCharsets.UTF_8);
	}

	private int awaitServerTotal(int expectedTotal) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TEST_TIMEOUT_SECONDS);
		int total = 0;
		while (System.nanoTime() < deadline) {
			synchronized (serverReceived) {
				total = 0;
				for (String payload : serverReceived) {
					total += payload.length();
				}
			}
			if (total >= expectedTotal) {
				return total;
			}
			Thread.sleep(10);
		}
		return total;
	}

	private boolean awaitServerValue(String expected) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TEST_TIMEOUT_SECONDS);
		while (System.nanoTime() < deadline) {
			if (serverReceived.contains(expected)) {
				return true;
			}
			Thread.sleep(10);
		}
		return serverReceived.contains(expected);
	}

	private static boolean awaitValue(List<String> values, CountDownLatch latch, String expected) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TEST_TIMEOUT_SECONDS);
		while (System.nanoTime() < deadline) {
			synchronized (values) {
				if (values.contains(expected)) {
					return true;
				}
			}
			latch.await(10, TimeUnit.MILLISECONDS);
		}
		synchronized (values) {
			return values.contains(expected);
		}
	}
}
