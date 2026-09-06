package pl.skidam.mcholepunch.server.netty;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;

import pl.skidam.mcholepunch.internal.crypto.MinecraftCrypt;
import pl.skidam.mcholepunch.internal.protocol.TransportFrameCodec;

class NettyHolepunchConnectionTest {
	@Test
	void framesPipelineWritesAsTransportData() throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel();
		NettyEncryption encryption = new NettyEncryption(MinecraftCrypt.newSecret());
		NettyHolepunchConnection connection = new NettyHolepunchConnection(channel, encryption, 1024);
		channel.pipeline().addLast(NettyHolepunchConnection.HANDLER_NAME, connection);

		channel.writeOutbound(Unpooled.wrappedBuffer(new byte[]{7, 8, 9}));

		ByteBuf frame = channel.readOutbound();
		byte[] expected = new byte[TransportFrameCodec.data(ByteBuffer.wrap(new byte[]{7, 8, 9})).remaining()];
		TransportFrameCodec.data(ByteBuffer.wrap(new byte[]{7, 8, 9})).get(expected);
		byte[] actual = new byte[frame.readableBytes()];
		frame.readBytes(actual);
		frame.release();
		assertArrayEquals(expected, actual);
		channel.finishAndReleaseAll();
	}

	@Test
	void rejectsWritesWhileTheCommitIsInFlight() throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel();
		NettyEncryption encryption = new NettyEncryption(MinecraftCrypt.newSecret());
		NettyHolepunchConnection connection = new NettyHolepunchConnection(channel, encryption, 1024);
		channel.pipeline().addLast(NettyHolepunchConnection.HANDLER_NAME, connection);

		connection.commitTransportUpgrade();
		channel.runPendingTasks();
		CompletionStage<Void> write = connection.write(ByteBuffer.wrap(new byte[]{1}));
		channel.runPendingTasks();

		assertThrows(ExecutionException.class, () -> write.toCompletableFuture().get(1, TimeUnit.SECONDS));
		channel.finishAndReleaseAll();
	}

	@Test
	void setsWritabilityWatermarksFromThePendingLimit() throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel();
		NettyEncryption encryption = new NettyEncryption(MinecraftCrypt.newSecret());
		NettyHolepunchConnection connection = new NettyHolepunchConnection(channel, encryption, 4096);
		channel.pipeline().addLast(NettyHolepunchConnection.HANDLER_NAME, connection);

		assertEquals(4096, channel.config().getWriteBufferWaterMark().high());
		channel.finishAndReleaseAll();
	}
}
