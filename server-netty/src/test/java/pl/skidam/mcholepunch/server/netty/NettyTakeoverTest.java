package pl.skidam.mcholepunch.server.netty;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

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

import pl.skidam.mcholepunch.internal.crypto.MinecraftCrypt;

class NettyTakeoverTest {
	@Test
	void preservesBytesBufferedBehindMinecraftFraming() throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel();
		channel.pipeline().addLast("splitter", new BufferingFrameDecoder());
		channel.pipeline().addLast("decoder", new ChannelInboundHandlerAdapter());
		channel.pipeline().addLast("prepender", new ChannelOutboundHandlerAdapter());
		channel.pipeline().addLast("encoder", new ChannelOutboundHandlerAdapter());
		channel.pipeline().addLast("packet_handler", new ChannelInboundHandlerAdapter());

		List<byte[]> received = new CopyOnWriteArrayList<>();
		NettyHolepunchConnection connection = new NettyHolepunchConnection(channel, new NettyEncryption(MinecraftCrypt.newSecret()), 1024);
		NettyTakeoverSpec spec = new NettyTakeoverSpec("splitter", "prepender", List.of("splitter", "decoder", "prepender", "encoder", "packet_handler"));

		byte[] applicationBytes = {1, 2, 3, 4};
		channel.writeInbound(Unpooled.wrappedBuffer(applicationBytes));
		NettyTakeover.activate(channel, spec, connection).join();

		assertNull(channel.pipeline().get("splitter"));
		assertNotNull(channel.pipeline().get(NettyHolepunchConnection.HANDLER_NAME));
		assertNotNull(channel.pipeline().get(spec.inboundWatchdogName()));
		assertTrue(received.isEmpty(), "bytes buffered behind vanilla framing before the takeover are discarded");
		channel.finishAndReleaseAll();
	}

	@Test
	void waitsForTakeoverWriteBeforeActivatingRawPipeline() throws Exception {
		EmbeddedChannel channel = minecraftPipeline();
		DeferredWriteHandler deferredWrite = new DeferredWriteHandler();
		channel.pipeline().addLast("deferred-write", deferredWrite);
		NettyHolepunchConnection connection = connection(channel);
		NettyTakeoverSpec spec = takeoverSpec();

		var takeover = NettyTakeover.sendTakeover(channel, Unpooled.wrappedBuffer(new byte[]{1}), spec, connection);

		assertFalse(takeover.isDone());
		assertNotNull(channel.pipeline().get("splitter"));
		assertNull(channel.pipeline().get(spec.inboundWatchdogName()));

		deferredWrite.promise.setSuccess();

		assertTrue(takeover.isDone());
		assertNotNull(channel.pipeline().get(spec.inboundWatchdogName()));
		assertNotNull(channel.pipeline().get(NettyHolepunchConnection.HANDLER_NAME));
		assertNull(channel.pipeline().get("splitter"));
		channel.finishAndReleaseAll();
	}

	@Test
	void dropsVanillaPacketsAfterTakeoverAndRemovesBundleHandlers() throws Exception {
		EmbeddedChannel channel = minecraftPipeline();
		channel.pipeline().addLast("unbundler", new ChannelOutboundHandlerAdapter());
		channel.pipeline().addLast("bundler", new ChannelOutboundHandlerAdapter());
		NettyHolepunchConnection connection = connection(channel);
		NettyTakeoverSpec spec = NettyTakeoverSpec.minecraftLogin();

		NettyTakeover.activate(channel, spec, connection).join();

		assertNull(channel.pipeline().get("unbundler"));
		assertNull(channel.pipeline().get("bundler"));
		assertNotNull(channel.pipeline().get("mcholepunch-raw-outbound"));
		channel.writeAndFlush(new Object()).syncUninterruptibly();
		assertTrue(channel.isOpen());
		channel.finishAndReleaseAll();
	}

	@Test
	void failedTakeoverWriteDoesNotActivateTheTransport() throws Exception {
		EmbeddedChannel channel = minecraftPipeline();
		DeferredWriteHandler deferredWrite = new DeferredWriteHandler();
		channel.pipeline().addLast("deferred-write", deferredWrite);
		NettyHolepunchConnection connection = connection(channel);
		NettyTakeoverSpec spec = takeoverSpec();

		var takeover = NettyTakeover.sendTakeover(channel, Unpooled.wrappedBuffer(new byte[]{1}), spec, connection);
		deferredWrite.promise.setFailure(new IllegalStateException("write failed"));

		assertTrue(takeover.isCompletedExceptionally());
		assertFalse(channel.isOpen());
		assertNull(channel.pipeline().get(spec.inboundWatchdogName()));
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
		return new NettyTakeoverSpec("splitter", "prepender", List.of("splitter", "decoder", "prepender", "encoder", "packet_handler"));
	}

	private static NettyHolepunchConnection connection(EmbeddedChannel channel) throws Exception {
		return new NettyHolepunchConnection(channel, new NettyEncryption(MinecraftCrypt.newSecret()), 1024);
	}

	private static final class DeferredWriteHandler extends ChannelOutboundHandlerAdapter {
		private ChannelPromise promise;

		@Override
		public void write(ChannelHandlerContext context, Object message, ChannelPromise promise) {
			ReferenceCountUtil.release(message);
			this.promise = promise;
		}
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

	private static final class BufferingFrameDecoder extends ByteToMessageDecoder {
		@Override
		protected void decode(ChannelHandlerContext context, ByteBuf input, List<Object> output) {}
	}
}
