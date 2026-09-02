package pl.skidam.mcholepunch.server.netty;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;

import pl.skidam.mcholepunch.HolepunchHandler;
import pl.skidam.mcholepunch.internal.crypto.MinecraftCrypt;
import pl.skidam.mcholepunch.internal.protocol.TransportFrameCodec;

class NettyTransportDecoderTest {
	@Test
	void acknowledgesReadyBeforeSwitchingToRawPipe() throws Exception {
		byte[] secret = MinecraftCrypt.newSecret();
		NettyEncryption serverEncryption = new NettyEncryption(secret);
		NettyEncryption clientEncryption = new NettyEncryption(secret);
		AtomicReference<byte[]> received = new AtomicReference<>();
		EmbeddedChannel channel = new EmbeddedChannel();
		NettyHolepunchConnection connection = new NettyHolepunchConnection(channel, Runnable::run, new HolepunchHandler() {
			@Override
			public void onRead(ByteBuffer data) {
				byte[] bytes = new byte[data.remaining()];
				data.get(bytes);
				received.set(bytes);
			}
		}, 1024, serverEncryption);
		channel.pipeline().addLast("transport-decoder", new NettyTransportDecoder(serverEncryption, connection, 1024));
		channel.pipeline().addLast("raw", new NettyHolepunchConnection.RawInboundHandler(connection));
		channel.pipeline().addLast("transport-encoder", new NettyCipherEncoder(serverEncryption));

		var prepared = connection.prepareTransportUpgrade().toCompletableFuture();
		channel.runPendingTasks();
		ByteBuf encryptedReady = channel.readOutbound();
		assertArrayEquals(new byte[]{1, 0}, clientEncryption.decrypt(readBytes(encryptedReady)));

		channel.writeInbound(Unpooled.wrappedBuffer(clientEncryption.encrypt(bytes(TransportFrameCodec.ready()))));
		ByteBuf encryptedAck = channel.readOutbound();
		assertArrayEquals(new byte[]{2, 0}, clientEncryption.decrypt(readBytes(encryptedAck)));
		channel.writeInbound(Unpooled.wrappedBuffer(clientEncryption.encrypt(bytes(TransportFrameCodec.ack()))));
		prepared.get();
		var committed = connection.commitTransportUpgrade().toCompletableFuture();
		channel.runPendingTasks();
		ByteBuf encryptedCommit = channel.readOutbound();
		assertArrayEquals(new byte[]{3, 0}, clientEncryption.decrypt(readBytes(encryptedCommit)));
		channel.writeInbound(Unpooled.wrappedBuffer(clientEncryption.encrypt(bytes(TransportFrameCodec.commit()))));
		committed.get();
		assertTrue(serverEncryption.isRaw());

		clientEncryption.activateRaw();
		byte[] payload = {4, 5, 6};
		byte[] encryptedData = clientEncryption.encrypt(payload);
		channel.writeInbound(Unpooled.wrappedBuffer(encryptedData, new byte[0]));

		assertArrayEquals(payload, received.get());
		channel.finishAndReleaseAll();
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
