package pl.skidam.mcholepunch.server.netty;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.ReferenceCountUtil;

import pl.skidam.mcholepunch.HolepunchConnection;
import pl.skidam.mcholepunch.HolepunchHandler;

class NettyTakeoverTest {
	@Test
	void preservesBytesBufferedBehindMinecraftFraming() {
		EmbeddedChannel channel = new EmbeddedChannel();
		channel.pipeline().addLast("splitter", new BufferingFrameDecoder());
		channel.pipeline().addLast("decoder", new ChannelInboundHandlerAdapter());
		channel.pipeline().addLast("prepender", new ChannelOutboundHandlerAdapter());
		channel.pipeline().addLast("encoder", new ChannelOutboundHandlerAdapter());
		channel.pipeline().addLast("packet_handler", new ChannelInboundHandlerAdapter());

		AtomicReference<byte[]> received = new AtomicReference<>();
		NettyHolepunchConnection connection = new NettyHolepunchConnection(channel, Runnable::run, new HolepunchHandler() {
			@Override
			public void onRead(ByteBuffer data) {
				byte[] bytes = new byte[data.remaining()];
				data.get(bytes);
				received.set(bytes);
			}
		}, 1024, null);
		NettyTakeoverSpec spec = NettyTakeoverSpec.builder().encryptionAnchors("splitter", "prepender").removalNames(List.of("splitter", "decoder", "prepender", "encoder", "packet_handler")).build();

		byte[] applicationBytes = {1, 2, 3, 4};
		channel.writeInbound(Unpooled.wrappedBuffer(applicationBytes));
		NettyTakeover.activate(channel, spec, connection).join();

		assertArrayEquals(applicationBytes, received.get());
		channel.finishAndReleaseAll();
	}

	@Test
	void waitsForTakeoverWriteBeforeActivatingRawPipeline() {
		EmbeddedChannel channel = minecraftPipeline();
		DeferredWriteHandler deferredWrite = new DeferredWriteHandler();
		channel.pipeline().addLast("deferred-write", deferredWrite);
		AtomicReference<Boolean> opened = new AtomicReference<>(false);
		NettyHolepunchConnection connection = connection(channel, opened);
		NettyTakeoverSpec spec = takeoverSpec();

		var takeover = NettyTakeover.sendTakeover(channel, Unpooled.wrappedBuffer(new byte[]{1}), spec, connection);

		assertFalse(takeover.isDone());
		assertFalse(opened.get());
		assertNotNull(channel.pipeline().get("splitter"));
		assertNull(channel.pipeline().get(spec.rawInboundName()));

		deferredWrite.promise.setSuccess();

		assertTrue(takeover.isDone());
		assertTrue(opened.get());
		assertNull(channel.pipeline().get("splitter"));
		assertNotNull(channel.pipeline().get(spec.rawInboundName()));
		channel.finishAndReleaseAll();
	}

	@Test
	void dropsVanillaPacketsAfterTakeoverAndRemovesBundleHandlers() {
		EmbeddedChannel channel = minecraftPipeline();
		channel.pipeline().addLast("unbundler", new ChannelOutboundHandlerAdapter());
		channel.pipeline().addLast("bundler", new ChannelOutboundHandlerAdapter());
		NettyHolepunchConnection connection = connection(channel, new AtomicReference<>());

		NettyTakeover.activate(channel, NettyTakeoverSpec.minecraftLogin(), connection).join();

		assertNull(channel.pipeline().get("unbundler"));
		assertNull(channel.pipeline().get("bundler"));
		assertNotNull(channel.pipeline().get("mcholepunch-raw-outbound"));
		channel.writeAndFlush(new Object()).syncUninterruptibly();
		assertTrue(channel.isOpen());
		channel.finishAndReleaseAll();
	}

	@Test
	void failedTakeoverWriteDoesNotOpenRawConnection() {
		EmbeddedChannel channel = minecraftPipeline();
		DeferredWriteHandler deferredWrite = new DeferredWriteHandler();
		channel.pipeline().addLast("deferred-write", deferredWrite);
		AtomicReference<Boolean> opened = new AtomicReference<>(false);
		NettyHolepunchConnection connection = connection(channel, opened);
		NettyTakeoverSpec spec = takeoverSpec();

		var takeover = NettyTakeover.sendTakeover(channel, Unpooled.wrappedBuffer(new byte[]{1}), spec, connection);
		deferredWrite.promise.setFailure(new IllegalStateException("write failed"));

		assertTrue(takeover.isCompletedExceptionally());
		assertFalse(opened.get());
		assertNotNull(channel.pipeline().get("splitter"));
		assertNull(channel.pipeline().get(spec.rawInboundName()));
		channel.finishAndReleaseAll();
	}

	private static EmbeddedChannel minecraftPipeline() {
		EmbeddedChannel channel = new EmbeddedChannel();
		channel.pipeline().addLast("splitter", new BufferingFrameDecoder());
		channel.pipeline().addLast("decoder", new ChannelInboundHandlerAdapter());
		channel.pipeline().addLast("prepender", new ChannelOutboundHandlerAdapter());
		channel.pipeline().addLast("encoder", new ChannelOutboundHandlerAdapter());
		channel.pipeline().addLast("packet_handler", new ChannelInboundHandlerAdapter());
		return channel;
	}

	private static NettyTakeoverSpec takeoverSpec() {
		return NettyTakeoverSpec.builder().encryptionAnchors("splitter", "prepender").removalNames(List.of("splitter", "decoder", "prepender", "encoder", "packet_handler")).build();
	}

	private static NettyHolepunchConnection connection(EmbeddedChannel channel, AtomicReference<Boolean> opened) {
		return new NettyHolepunchConnection(channel, Runnable::run, new HolepunchHandler() {
			@Override
			public void onOpen(HolepunchConnection connection) {
				opened.set(true);
			}
		}, 1024, null);
	}

	private static final class DeferredWriteHandler extends ChannelOutboundHandlerAdapter {
		private ChannelPromise promise;

		@Override
		public void write(ChannelHandlerContext context, Object message, ChannelPromise promise) {
			ReferenceCountUtil.release(message);
			this.promise = promise;
		}
	}

	private static final class BufferingFrameDecoder extends ByteToMessageDecoder {
		@Override
		protected void decode(ChannelHandlerContext context, ByteBuf input, List<Object> output) {}
	}
}
