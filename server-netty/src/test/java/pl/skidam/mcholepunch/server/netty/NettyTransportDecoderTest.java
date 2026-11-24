package pl.skidam.mcholepunch.server.netty;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;

import pl.skidam.mcholepunch.internal.crypto.MinecraftCrypt;
import pl.skidam.mcholepunch.internal.crypto.TransportEncryption;
import pl.skidam.mcholepunch.internal.protocol.TransportFrameCodec;

class NettyTransportDecoderTest {
	@Test
	void carriesDataThenCommitsToTheRawPipe() throws Exception {
		byte[] secret = MinecraftCrypt.newSecret();
		NettyEncryption serverEncryption = new NettyEncryption(secret);
		TransportEncryption clientEncryption = TransportEncryption.server(secret);
		List<byte[]> received = new ArrayList<>();
		EmbeddedChannel channel = new EmbeddedChannel();
		NettyHolepunchConnection connection = new NettyHolepunchConnection(channel, serverEncryption, 1024);
		channel.pipeline().addLast("transport-decoder", new NettyTransportDecoder(serverEncryption, connection, 1024));
		channel.pipeline().addLast("collector", new CollectingHandler(received));
		channel.pipeline().addLast("transport-encoder", new NettyCipherEncoder(serverEncryption));
		channel.pipeline().addLast(NettyHolepunchConnection.HANDLER_NAME, connection);

		channel.writeInbound(Unpooled.wrappedBuffer(clientEncryption.encrypt(bytes(TransportFrameCodec.data(ByteBuffer.wrap("framed".getBytes(StandardCharsets.UTF_8)))))));
		assertEquals(List.of("framed"), texts(received));

		var committed = connection.commitTransportUpgrade().toCompletableFuture();
		channel.runPendingTasks();
		ByteBuf encryptedCommit = channel.readOutbound();
		assertArrayEquals(new byte[]{1, 0}, clientEncryption.decrypt(readBytes(encryptedCommit)), "COMMIT must be wire byte 1 with an empty payload");

		channel.writeInbound(Unpooled.wrappedBuffer(clientEncryption.encrypt(bytes(TransportFrameCodec.commit()))));
		committed.get();
		assertTrue(serverEncryption.isRaw());
		assertNull(channel.pipeline().get(NettyTransportDecoder.class), "decoder should remove itself once raw");
		assertNull(channel.pipeline().get(NettyCipherEncoder.class), "cipher encoder should be removed once raw");

		clientEncryption.activateRaw();
		channel.writeInbound(Unpooled.wrappedBuffer("raw".getBytes(StandardCharsets.UTF_8)));

		assertEquals(List.of("framed", "raw"), texts(received));
		channel.finishAndReleaseAll();
	}

	@Test
	void passesRawBytesThroughOnceCommitted() throws Exception {
		byte[] secret = MinecraftCrypt.newSecret();
		NettyEncryption serverEncryption = new NettyEncryption(secret);
		TransportEncryption clientEncryption = TransportEncryption.server(secret);
		List<byte[]> received = new ArrayList<>();
		EmbeddedChannel channel = new EmbeddedChannel();
		NettyHolepunchConnection connection = new NettyHolepunchConnection(channel, serverEncryption, 1024);
		channel.pipeline().addLast("transport-decoder", new NettyTransportDecoder(serverEncryption, connection, 1024));
		channel.pipeline().addLast("collector", new CollectingHandler(received));
		channel.pipeline().addLast("transport-encoder", new NettyCipherEncoder(serverEncryption));
		channel.pipeline().addLast(NettyHolepunchConnection.HANDLER_NAME, connection);

		var committed = connection.commitTransportUpgrade().toCompletableFuture();
		channel.runPendingTasks();
		channel.readOutbound();
		channel.writeInbound(Unpooled.wrappedBuffer(clientEncryption.encrypt(bytes(TransportFrameCodec.commit()))));
		committed.get();

		channel.writeInbound(Unpooled.wrappedBuffer(new byte[]{4, 5, 6}));
		assertArrayEquals(new byte[]{4, 5, 6}, received.get(0));
		channel.finishAndReleaseAll();
	}

	@Test
	void deliversRawTailCoalescedBehindThePeerCommit() throws Exception {
		byte[] secret = MinecraftCrypt.newSecret();
		NettyEncryption serverEncryption = new NettyEncryption(secret);
		TransportEncryption clientEncryption = TransportEncryption.server(secret);
		List<byte[]> received = new ArrayList<>();
		EmbeddedChannel channel = new EmbeddedChannel();
		NettyHolepunchConnection connection = new NettyHolepunchConnection(channel, serverEncryption, 1024);
		channel.pipeline().addLast("transport-decoder", new NettyTransportDecoder(serverEncryption, connection, 1024));
		channel.pipeline().addLast("collector", new CollectingHandler(received));
		channel.pipeline().addLast("transport-encoder", new NettyCipherEncoder(serverEncryption));
		channel.pipeline().addLast(NettyHolepunchConnection.HANDLER_NAME, connection);

		var committed = connection.commitTransportUpgrade().toCompletableFuture();
		channel.runPendingTasks();
		channel.readOutbound();

		// The peer flips as soon as our COMMIT reaches it and its raw bytes can share the COMMIT's
		// own TCP segment: the decoder must flip on the COMMIT and hand the untouched tail to the
		// raw pipe instead of decrypting it as a frame.
		ByteBuffer coalesced = ByteBuffer.allocate(512);
		coalesced.put(clientEncryption.encrypt(bytes(TransportFrameCodec.commit())));
		byte[] raw = "raw after the flip".getBytes(StandardCharsets.UTF_8);
		coalesced.put(raw);
		coalesced.flip();
		channel.writeInbound(Unpooled.wrappedBuffer(coalesced));
		committed.get();

		assertTrue(serverEncryption.isRaw());
		assertEquals(1, received.size());
		assertArrayEquals(raw, received.get(0));
		channel.finishAndReleaseAll();
	}

	private static List<String> texts(List<byte[]> received) {
		return received.stream().map(bytes -> new String(bytes, StandardCharsets.UTF_8)).toList();
	}

	private static final class CollectingHandler extends ChannelInboundHandlerAdapter {
		private final List<byte[]> received;

		CollectingHandler(List<byte[]> received) {
			this.received = received;
		}

		@Override
		public void channelRead(ChannelHandlerContext context, Object message) {
			ByteBuf buffer = (ByteBuf) message;
			try {
				byte[] bytes = new byte[buffer.readableBytes()];
				buffer.readBytes(bytes);
				received.add(bytes);
			} finally {
				buffer.release();
			}
		}
	}

	private static byte[] bytes(ByteBuffer buffer) {
		byte[] result = new byte[buffer.remaining()];
		buffer.get(result);
		return result;
	}

	private static byte[] readBytes(ByteBuf buffer) {
		try {
			byte[] result = new byte[buffer.readableBytes()];
			buffer.readBytes(result);
			return result;
		} finally {
			buffer.release();
		}
	}
}
