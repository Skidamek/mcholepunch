package pl.skidam.mcholepunch.server.netty;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.handler.flow.FlowControlHandler;
import io.netty.util.ReferenceCountUtil;

import pl.skidam.mcholepunch.HolepunchFailure;
import pl.skidam.mcholepunch.internal.protocol.TransportFrame;

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
					result.complete(null);
				} else {
					connection.closeWith(new HolepunchFailure(HolepunchFailure.Kind.IO, "takeover activation failed", error));
					result.completeExceptionally(error);
				}
			});
		});
		return result;
	}

	/**
	 * Installs the mcholepunch transport handlers on the taken-over channel and strips the vanilla
	 * handlers the takeover displaced. Stripping happens here and not later: the client may speak
	 * transport data as soon as it sees the takeover query, so the application pipeline must be
	 * installed on a channel that is already transport-only.
	 */
	public static CompletableFuture<Void> activate(Channel channel, NettyTakeoverSpec spec, NettyHolepunchConnection connection) {
		CompletableFuture<Void> result = new CompletableFuture<>();
		Runnable activation = () -> {
			try {
				ChannelPipeline pipeline = channel.pipeline();
				if (connection.encryption() != null && pipeline.get(spec.cipherDecoderName()) != null) {
					pipeline.replace(spec.cipherDecoderName(), spec.cipherDecoderName(), new NettyTransportDecoder(connection.encryption(), connection, TransportFrame.MAX_FRAME_BYTES));
				}
				// Outbound traversal runs tail-ward-to-head, so the framer must sit closer to the
				// tail than the cipher encoder: frames are written first, then encrypted.
				if (pipeline.get(NettyHolepunchConnection.HANDLER_NAME) == null) {
					if (pipeline.get(spec.cipherEncoderName()) != null) {
						pipeline.addAfter(spec.cipherEncoderName(), NettyHolepunchConnection.HANDLER_NAME, connection);
					} else {
						pipeline.addLast(NettyHolepunchConnection.HANDLER_NAME, connection);
					}
				}
				if (pipeline.get(spec.rawOutboundName()) == null) {
					if (pipeline.get(spec.cipherEncoderName()) != null) {
						pipeline.addAfter(spec.cipherEncoderName(), spec.rawOutboundName(), new PassThroughOutboundHandler());
					} else {
						pipeline.addLast(spec.rawOutboundName(), new PassThroughOutboundHandler());
					}
				}
				if (pipeline.get(spec.inboundWatchdogName()) == null) {
					pipeline.addLast(spec.inboundWatchdogName(), new NettyHolepunchConnection.InboundWatchdog(connection));
				}
				removeIfPresent(pipeline, spec, spec.negotiatorName());
				for (String name : spec.removalNames()) {
					if (!name.equals(spec.inboundEncryptionAnchor())) {
						removeIfPresent(pipeline, spec, name);
					}
				}
				removeIfPresent(pipeline, spec, spec.inboundEncryptionAnchor());
				// Vanilla parks login connections with autoRead off and a FlowControlHandler that
				// holds packets for a login listener the takeover replaced. Without draining that
				// stage, exactly the first inbound message reaches the application and the rest
				// queues forever behind a read() nobody issues.
				for (String name : pipeline.names()) {
					if (pipeline.get(name) instanceof FlowControlHandler) {
						pipeline.remove(name);
					}
				}
				channel.config().setAutoRead(true);
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
		if (!spec.owns(name) && pipeline.get(name) != null) {
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
