package pl.skidam.mcholepunch.server.netty;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;

import pl.skidam.mcholepunch.internal.crypto.MinecraftCrypt;
import pl.skidam.mcholepunch.internal.protocol.TransportFrame;
import pl.skidam.mcholepunch.internal.protocol.TransportFrameCodec;
import pl.skidam.mcholepunch.internal.protocol.VarInts;

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
	void swallowsPacketShapedWritesFromLeftoverVanillaState() throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel();
		NettyEncryption encryption = new NettyEncryption(MinecraftCrypt.newSecret());
		NettyHolepunchConnection connection = new NettyHolepunchConnection(channel, encryption, 1024);
		channel.pipeline().addLast(NettyHolepunchConnection.HANDLER_NAME, connection);

		channel.writeOutbound(new Object());

		assertTrue(channel.isOpen());
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
	void aWriteRejectedByTheCommitWindowKeepsTheConnectionOpen() throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel();
		NettyEncryption encryption = new NettyEncryption(MinecraftCrypt.newSecret());
		NettyHolepunchConnection connection = new NettyHolepunchConnection(channel, encryption, 1024);
		channel.pipeline().addLast(NettyHolepunchConnection.HANDLER_NAME, connection);

		CompletionStage<Void> commit = connection.commitTransportUpgrade();
		channel.runPendingTasks();
		CompletionStage<Void> rejected = connection.write(ByteBuffer.wrap(new byte[]{1}));
		channel.runPendingTasks();
		assertThrows(ExecutionException.class, () -> rejected.toCompletableFuture().get(1, TimeUnit.SECONDS));

		assertTrue(channel.isOpen(), "a commit-window rejection is policy for one write, not a transport failure");
		// The peer's COMMIT arrives after the rejection; the handoff must still complete.
		connection.receiveControl();
		commit.toCompletableFuture().get(1, TimeUnit.SECONDS);
		assertTrue(connection.isRaw());
		assertTrue(channel.isOpen());
		channel.finishAndReleaseAll();
	}

	@Test
	void splitsWritesAboveTheTransportFrameCap() throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel();
		NettyEncryption encryption = new NettyEncryption(MinecraftCrypt.newSecret());
		NettyHolepunchConnection connection = new NettyHolepunchConnection(channel, encryption, TransportFrame.MAX_FRAME_BYTES);
		channel.pipeline().addLast(NettyHolepunchConnection.HANDLER_NAME, connection);

		int overCap = TransportFrame.MAX_FRAME_BYTES + 1;
		channel.writeOutbound(Unpooled.wrappedBuffer(new byte[overCap]));

		ByteBuf first = channel.readOutbound();
		assertEquals(TransportFrame.Type.DATA.wire(), first.readByte(), "the first chunk must carry the data frame type");
		assertEquals(TransportFrame.MAX_FRAME_BYTES, VarInts.read(first.nioBuffer(first.readerIndex(), first.readableBytes())));
		assertEquals(TransportFrame.MAX_FRAME_BYTES, first.readableBytes() - VarInts.encodedLength(TransportFrame.MAX_FRAME_BYTES), "the first chunk's payload fills the cap");
		first.release();
		ByteBuf second = channel.readOutbound();
		assertEquals(TransportFrame.Type.DATA.wire(), second.readByte(), "the last chunk must carry the data frame type");
		assertEquals(1, VarInts.read(second.nioBuffer(second.readerIndex(), second.readableBytes())));
		assertEquals(1, second.readableBytes() - VarInts.encodedLength(1), "the last chunk carries the remainder");
		second.release();
		assertNull(channel.readOutbound(), "no frames beyond the split may appear");
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
