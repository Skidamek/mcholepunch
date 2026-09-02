package pl.skidam.mcholepunch.server.netty;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.util.ReferenceCountUtil;

import pl.skidam.mcholepunch.HolepunchFailure;

public final class NettyTakeover {
	private NettyTakeover() {}

	public static NettyEncryption installEncryption(Channel channel, NettyTakeoverSpec spec, NettyEncryption encryption) throws Exception {
		Objects.requireNonNull(channel, "channel");
		Objects.requireNonNull(encryption, "encryption");
		ChannelPipeline pipeline = channel.pipeline();
		if (pipeline.get(spec.inboundEncryptionAnchor()) == null || pipeline.get(spec.outboundEncryptionAnchor()) == null) {
			throw new IllegalStateException("Minecraft pipeline encryption anchors are missing");
		}
		if (pipeline.get(spec.cipherDecoderName()) == null) {
			pipeline.addBefore(spec.inboundEncryptionAnchor(), spec.cipherDecoderName(), new NettyCipherDecoder(encryption));
		}
		if (pipeline.get(spec.cipherEncoderName()) == null) {
			pipeline.addBefore(spec.outboundEncryptionAnchor(), spec.cipherEncoderName(), new NettyCipherEncoder(encryption));
		}
		return encryption;
	}

	public static CompletableFuture<Void> sendTakeover(Channel channel, ByteBuf queryPacket, NettyTakeoverSpec spec, NettyHolepunchConnection connection) {
		CompletableFuture<Void> result = new CompletableFuture<>();
		ChannelFuture write = channel.writeAndFlush(queryPacket);
		write.addListener(future -> {
			if (!future.isSuccess()) {
				result.completeExceptionally(future.cause());
				connection.closeWith(new HolepunchFailure(HolepunchFailure.Kind.IO, "takeover query write failed", future.cause()));
				return;
			}
			activate(channel, spec, connection).whenComplete((ignored, error) -> {
				if (error == null) {
					connection.open();
					result.complete(null);
				} else {
					connection.closeWith(new HolepunchFailure(HolepunchFailure.Kind.IO, "takeover activation failed", error));
					result.completeExceptionally(error);
				}
			});
		});
		return result;
	}

	public static CompletableFuture<Void> activate(Channel channel, NettyTakeoverSpec spec, NettyHolepunchConnection connection) {
		CompletableFuture<Void> result = new CompletableFuture<>();
		Runnable activation = () -> {
			try {
				ChannelPipeline pipeline = channel.pipeline();
				if (connection.encryption() != null && pipeline.get(spec.cipherDecoderName()) != null) {
					pipeline.replace(spec.cipherDecoderName(), spec.cipherDecoderName(), new NettyTransportDecoder(connection.encryption(), connection, connection.maximumFrameLength()));
				}
				if (pipeline.get(spec.rawInboundName()) == null) {
					pipeline.addAfter(spec.inboundEncryptionAnchor(), spec.rawInboundName(), new NettyHolepunchConnection.RawInboundHandler(connection));
				}
				if (pipeline.get(spec.rawOutboundName()) == null) {
					if (pipeline.get(spec.cipherEncoderName()) != null) {
						pipeline.addAfter(spec.cipherEncoderName(), spec.rawOutboundName(), new PassThroughOutboundHandler());
					} else {
						pipeline.addLast(spec.rawOutboundName(), new PassThroughOutboundHandler());
					}
				}
				removeIfPresent(pipeline, spec, spec.negotiatorName());
				for (String name : spec.removalNames()) {
					if (!name.equals(spec.inboundEncryptionAnchor())) {
						removeIfPresent(pipeline, spec, name);
					}
				}
				removeIfPresent(pipeline, spec, spec.inboundEncryptionAnchor());
				result.complete(null);
			} catch (Throwable throwable) {
				result.completeExceptionally(throwable);
			}
		};
		if (channel.eventLoop().inEventLoop()) {
			activation.run();
		} else {
			channel.eventLoop().execute(activation);
		}
		return result;
	}

	private static void removeIfPresent(ChannelPipeline pipeline, NettyTakeoverSpec spec, String name) {
		if (!name.equals(spec.cipherDecoderName()) && !name.equals(spec.cipherEncoderName()) && !name.equals(spec.rawInboundName()) && !name.equals(spec.rawOutboundName()) && pipeline.get(name) != null) {
			pipeline.remove(name);
		}
	}

	private static final class PassThroughOutboundHandler extends ChannelOutboundHandlerAdapter {
		@Override
		public void write(ChannelHandlerContext context, Object message, ChannelPromise promise) {
			if (message instanceof ByteBuf) {
				context.write(message, promise);
				return;
			}
			// Vanilla login still sits on the hijacked Connection and may write Packet objects
			// (ClientboundLoginDisconnectPacket on timeout). Epoll only accepts ByteBuf after takeover.
			ReferenceCountUtil.release(message);
			promise.setSuccess();
		}
	}
}
