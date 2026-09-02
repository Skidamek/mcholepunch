package pl.skidam.mcholepunch.server.netty;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;

import pl.skidam.mcholepunch.HolepunchHandler;
import pl.skidam.mcholepunch.internal.crypto.MinecraftCrypt;
import pl.skidam.mcholepunch.internal.crypto.TransportEncryption;
import pl.skidam.mcholepunch.internal.protocol.TransportFrameCodec;

class NettyTransportDecoderTest {
	@Test
	void carriesDataThenCommitsToTheRawPipe() throws Exception {
		byte[] secret = MinecraftCrypt.newSecret();
		NettyEncryption serverEncryption = new NettyEncryption(secret);
		TransportEncryption clientEncryption = TransportEncryption.server(secret);
		AtomicReference<String> framed = new AtomicReference<>();
		AtomicReference<String> raw = new AtomicReference<>();
		EmbeddedChannel channel = new EmbeddedChannel();
		NettyHolepunchConnection connection = new NettyHolepunchConnection(channel, Runnable::run, new HolepunchHandler() {
			@Override
			public void onRead(ByteBuffer data) {
				framed.set(remaining(data));
			}

			@Override
			public void onRawRead(ByteBuffer data) {
				raw.set(remaining(data));
			}
		}, 1024, serverEncryption);
		channel.pipeline().addLast("transport-decoder", new NettyTransportDecoder(serverEncryption, connection, 1024));
		channel.pipeline().addLast("raw", new NettyHolepunchConnection.RawInboundHandler(connection));
		channel.pipeline().addLast("transport-encoder", new NettyCipherEncoder(serverEncryption));

		channel.writeInbound(Unpooled.wrappedBuffer(clientEncryption.encrypt(bytes(TransportFrameCodec.data(ByteBuffer.wrap("framed".getBytes(StandardCharsets.UTF_8)))))));
		assertEquals("framed", framed.get());

		var committed = connection.commitTransportUpgrade().toCompletableFuture();
		channel.runPendingTasks();
		ByteBuf encryptedCommit = channel.readOutbound();
		assertArrayEquals(new byte[]{1, 0}, clientEncryption.decrypt(readBytes(encryptedCommit)), "COMMIT must be wire byte 1 with an empty payload");

		channel.writeInbound(Unpooled.wrappedBuffer(clientEncryption.encrypt(bytes(TransportFrameCodec.commit()))));
		committed.get();
		assertTrue(serverEncryption.isRaw());

		clientEncryption.activateRaw();
		byte[] payload = "raw".getBytes(StandardCharsets.UTF_8);
		channel.writeInbound(Unpooled.wrappedBuffer(payload));

		assertEquals("raw", raw.get());
		channel.finishAndReleaseAll();
	}

	@Test
	void passesRawBytesThroughOnceCommitted() throws Exception {
		byte[] secret = MinecraftCrypt.newSecret();
		NettyEncryption serverEncryption = new NettyEncryption(secret);
		TransportEncryption clientEncryption = TransportEncryption.server(secret);
		AtomicReference<String> raw = new AtomicReference<>();
		EmbeddedChannel channel = new EmbeddedChannel();
		NettyHolepunchConnection connection = new NettyHolepunchConnection(channel, Runnable::run, new HolepunchHandler() {
			@Override
			public void onRawRead(ByteBuffer data) {
				raw.set(remaining(data));
			}
		}, 1024, serverEncryption);
		channel.pipeline().addLast("transport-decoder", new NettyTransportDecoder(serverEncryption, connection, 1024));
		channel.pipeline().addLast("raw", new NettyHolepunchConnection.RawInboundHandler(connection));
		channel.pipeline().addLast("transport-encoder", new NettyCipherEncoder(serverEncryption));

		var committed = connection.commitTransportUpgrade().toCompletableFuture();
		channel.runPendingTasks();
		channel.readOutbound();
		channel.writeInbound(Unpooled.wrappedBuffer(clientEncryption.encrypt(bytes(TransportFrameCodec.commit()))));
		committed.get();

		channel.writeInbound(Unpooled.wrappedBuffer(new byte[]{4, 5, 6}));
		assertArrayEquals(new byte[]{4, 5, 6}, raw.get().getBytes(StandardCharsets.UTF_8));
		channel.finishAndReleaseAll();
	}

	private static String remaining(ByteBuffer data) {
		byte[] bytes = new byte[data.remaining()];
		data.duplicate().get(bytes);
		return new String(bytes, StandardCharsets.UTF_8);
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
