package pl.skidam.mcholepunch.server.netty;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.util.ReferenceCountUtil;

import pl.skidam.mcholepunch.HolepunchFailure;
import pl.skidam.mcholepunch.HolepunchHandler;
import pl.skidam.mcholepunch.internal.DebugLog;
import pl.skidam.mcholepunch.internal.crypto.MinecraftCrypt;
import pl.skidam.mcholepunch.internal.protocol.HolepunchMarker;
import pl.skidam.mcholepunch.internal.protocol.LoginCodec;
import pl.skidam.mcholepunch.server.HolepunchServerRegistry;

public final class NettyLoginNegotiator {
	public static final Duration DEFAULT_RESPONSE_TIMEOUT = Duration.ofSeconds(10);
	/** The challenge mirrors vanilla's login verify token, which is four bytes. */
	private static final int CHALLENGE_BYTES = 4;

	private final Channel channel;
	private final KeyPair keyPair;
	private final String username;
	private final SocketAddress remoteAddress;
	private final HolepunchMarker marker;
	private final HolepunchServerRegistry.Registration registration;
	private final NettyTakeoverSpec takeoverSpec;
	private final Duration responseTimeout;
	private final byte[] challenge;
	private State state = State.NEW;
	private ScheduledFuture<?> responseTimeoutFuture;
	private ScheduledFuture<?> setupTimeoutFuture;
	private NettyHolepunchConnection connection;

	public NettyLoginNegotiator(Channel channel, KeyPair keyPair, String username, SocketAddress remoteAddress, HolepunchMarker marker, HolepunchServerRegistry.Registration registration, NettyTakeoverSpec takeoverSpec) {
		this(channel, keyPair, username, remoteAddress, marker, registration, takeoverSpec, DEFAULT_RESPONSE_TIMEOUT);
	}

	NettyLoginNegotiator(Channel channel, KeyPair keyPair, String username, SocketAddress remoteAddress, HolepunchMarker marker, HolepunchServerRegistry.Registration registration, NettyTakeoverSpec takeoverSpec,
			Duration responseTimeout) {
		this.channel = Objects.requireNonNull(channel, "channel");
		this.keyPair = Objects.requireNonNull(keyPair, "keyPair");
		this.username = Objects.requireNonNull(username, "username");
		this.remoteAddress = Objects.requireNonNull(remoteAddress, "remoteAddress");
		this.marker = Objects.requireNonNull(marker, "marker");
		this.registration = Objects.requireNonNull(registration, "registration");
		this.takeoverSpec = Objects.requireNonNull(takeoverSpec, "takeoverSpec");
		this.responseTimeout = requirePositive(responseTimeout, "responseTimeout");
		this.challenge = MinecraftCrypt.randomBytes(CHALLENGE_BYTES);
	}

	public void start() {
		executeOnEventLoop(this::startOnEventLoop);
	}

	private void startOnEventLoop() {
		if (state != State.NEW) {
			return;
		}
		try {
			ChannelPipeline pipeline = channel.pipeline();
			if (pipeline.get(takeoverSpec.inboundEncryptionAnchor()) == null) {
				throw new IllegalStateException("Minecraft frame decoder is missing");
			}
			if (pipeline.get(takeoverSpec.negotiatorName()) != null) {
				throw new IllegalStateException("holepunch login negotiator is already installed");
			}
			pipeline.addAfter(takeoverSpec.inboundEncryptionAnchor(), takeoverSpec.negotiatorName(), new ResponseHandler());
			state = State.REQUEST_SENT;
			responseTimeoutFuture = channel.eventLoop().schedule(() -> fail(HolepunchFailure.Kind.TIMEOUT, "login encryption response timed out", null), responseTimeout.toNanos(), TimeUnit.NANOSECONDS);
			ByteBuffer request = LoginCodec.encryptionRequestPacket(keyPair.getPublic().getEncoded(), challenge, false);
			channel.writeAndFlush(Unpooled.wrappedBuffer(request)).addListener(future -> {
				if (!future.isSuccess()) {
					fail(HolepunchFailure.Kind.IO, "unable to send login encryption request", future.cause());
				}
			});
		} catch (Throwable throwable) {
			fail(HolepunchFailure.Kind.IO, "unable to start login encryption", throwable);
		}
	}

	private void handleResponse(ByteBuf input) {
		if (state != State.REQUEST_SENT) {
			fail(HolepunchFailure.Kind.UNEXPECTED_PACKET, "unexpected packet during login encryption", null);
			return;
		}
		try {
			LoginCodec.EncryptionResponse response = LoginCodec.readEncryptionResponse(input.nioBuffer());
			byte[] secret = MinecraftCrypt.decryptLoginSecret(keyPair.getPrivate(), response.encryptedSecret(), response.encryptedChallenge(), challenge);
			cancel(responseTimeoutFuture);
			state = State.PREPARING_HANDLER;
			prepareHandler(secret);
		} catch (IOException exception) {
			fail(HolepunchFailure.Kind.UNEXPECTED_PACKET, "invalid login encryption response", exception);
		} catch (GeneralSecurityException exception) {
			fail(HolepunchFailure.Kind.IO, "login encryption failed", exception);
		}
	}

	private void prepareHandler(byte[] secret) {
		CompletableFuture<HolepunchHandler> preparation = CompletableFuture.supplyAsync(() -> {
			try {
				return Objects.requireNonNull(registration.factory().create(username, remoteAddress, marker), "application handler");
			} catch (Exception exception) {
				throw new HandlerPreparationException(exception);
			}
		}, registration.executor());
		setupTimeoutFuture = channel.eventLoop().schedule(() -> fail(HolepunchFailure.Kind.TIMEOUT, "holepunch handler preparation timed out", null), registration.handlerSetupTimeout().toNanos(), TimeUnit.NANOSECONDS);
		preparation.whenComplete((handler, error) -> executeOnEventLoop(() -> {
			if (state != State.PREPARING_HANDLER) {
				return;
			}
			cancel(setupTimeoutFuture);
			if (error != null) {
				fail(HolepunchFailure.Kind.IO, "unable to prepare holepunch handler", unwrap(error));
				return;
			}
			beginTakeover(secret, handler);
		}));
	}

	private void beginTakeover(byte[] secret, HolepunchHandler handler) {
		try {
			NettyEncryption encryption = new NettyEncryption(secret);
			connection = new NettyHolepunchConnection(channel, registration.executor(), handler, registration.maximumPendingWriteBytes(), encryption);
			channel.closeFuture().addListener(ignored -> connection.closeWith(new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "Minecraft channel closed", null)));
			NettyTakeover.installEncryption(channel, takeoverSpec, encryption);
			ByteBuffer query = LoginCodec.takeoverQueryPacket(0);
			state = State.TAKEOVER_WRITE;
			NettyTakeover.sendTakeover(channel, Unpooled.wrappedBuffer(query), takeoverSpec, connection).whenComplete((ignored, error) -> executeOnEventLoop(() -> {
				if (state != State.TAKEOVER_WRITE) {
					return;
				}
				if (error != null) {
					state = State.CLOSED;
					return;
				}
				state = State.OPEN;
			}));
		} catch (Throwable throwable) {
			fail(HolepunchFailure.Kind.IO, "unable to activate holepunch connection", throwable);
		}
	}

	private void fail(HolepunchFailure.Kind kind, String message, Throwable cause) {
		if (!channel.eventLoop().inEventLoop()) {
			executeOnEventLoop(() -> fail(kind, message, cause));
			return;
		}
		if (state == State.CLOSED || state == State.OPEN) {
			return;
		}
		state = State.CLOSED;
		cancel(responseTimeoutFuture);
		cancel(setupTimeoutFuture);
		ChannelPipeline pipeline = channel.pipeline();
		if (pipeline.get(takeoverSpec.negotiatorName()) != null) {
			pipeline.remove(takeoverSpec.negotiatorName());
		}
		HolepunchFailure failure = new HolepunchFailure(kind, message, cause);
		if (connection != null) {
			connection.closeWith(failure);
		} else {
			DebugLog.failure("SERVER", 0, message, cause);
			channel.close();
		}
	}

	private void executeOnEventLoop(Runnable action) {
		if (channel.eventLoop().inEventLoop()) {
			action.run();
		} else {
			channel.eventLoop().execute(action);
		}
	}

	private static Duration requirePositive(Duration duration, String name) {
		Objects.requireNonNull(duration, name);
		if (duration.isZero() || duration.isNegative()) {
			throw new IllegalArgumentException(name + " must be positive");
		}
		return duration;
	}

	private static void cancel(ScheduledFuture<?> future) {
		if (future != null) {
			future.cancel(false);
		}
	}

	private static Throwable unwrap(Throwable throwable) {
		Throwable cause = throwable.getCause();
		return cause == null ? throwable : cause;
	}

	private final class ResponseHandler extends ChannelInboundHandlerAdapter {
		@Override
		public void channelRead(ChannelHandlerContext context, Object message) {
			if (!(message instanceof ByteBuf input)) {
				ReferenceCountUtil.release(message);
				fail(HolepunchFailure.Kind.UNEXPECTED_PACKET, "unexpected login pipeline message", null);
				return;
			}
			try {
				handleResponse(input);
			} finally {
				input.release();
			}
		}

		@Override
		public void channelInactive(ChannelHandlerContext context) {
			fail(HolepunchFailure.Kind.CLOSED, "channel closed during login negotiation", null);
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
			fail(HolepunchFailure.Kind.IO, "login negotiation pipeline failed", cause);
		}
	}

	private static final class HandlerPreparationException extends RuntimeException {
		private HandlerPreparationException(Throwable cause) {
			super(cause);
		}
	}

	private enum State {
		NEW, REQUEST_SENT, PREPARING_HANDLER, TAKEOVER_WRITE, OPEN, CLOSED
	}
}
