package pl.skidam.mcholepunch.server.netty;

import java.io.IOException;
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
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.DecoderException;
import io.netty.util.ReferenceCountUtil;

import pl.skidam.mcholepunch.HolepunchFailure;
import pl.skidam.mcholepunch.internal.DebugLog;
import pl.skidam.mcholepunch.internal.crypto.MinecraftCrypt;
import pl.skidam.mcholepunch.internal.protocol.LoginCodec;
import pl.skidam.mcholepunch.internal.protocol.TransportFrame;
import pl.skidam.mcholepunch.server.netty.NettyChannelRegistry.Registration;

/**
 * Owns the taken-over channel from the moment the holepunch marker is detected: the vanilla
 * pipeline is stripped and replaced with our own vanilla-shaped negotiation framing, and after
 * the takeover query the coordinated transport takes over. The wire stays byte-for-byte
 * vanilla-shaped throughout.
 */
public final class NettyLoginNegotiator {
	public static final Duration DEFAULT_RESPONSE_TIMEOUT = Duration.ofSeconds(10);
	/**
	 * The negotiation phase only carries the encryption exchange, under 1.2 KB per frame; a small
	 * cap keeps a client drip-feeding one oversized frame from copying a growing buffer on every
	 * read for the whole response window.
	 */
	private static final int NEGOTIATION_MAX_FRAME_BYTES = 64 * 1024;
	/** The challenge mirrors vanilla's login verify token, which is four bytes. */
	private static final int CHALLENGE_BYTES = 4;

	private static final String RESPONSE_HANDLER_NAME = "mcholepunch-response-handler";
	public static final String CIPHER_ENCODER_NAME = "mcholepunch-cipher-encoder";
	private static final String TRANSPORT_DECODER_NAME = "mcholepunch-transport-decoder";

	private final Channel channel;
	private final KeyPair keyPair;
	private final Registration registration;
	private final Duration responseTimeout;
	private final byte[] challenge;
	private State state = State.NEW;
	private ScheduledFuture<?> responseTimeoutFuture;
	private NettyHolepunchConnection connection;

	public NettyLoginNegotiator(Channel channel, KeyPair keyPair, Registration registration) {
		this(channel, keyPair, registration, DEFAULT_RESPONSE_TIMEOUT);
	}

	NettyLoginNegotiator(Channel channel, KeyPair keyPair, Registration registration, Duration responseTimeout) {
		this.channel = Objects.requireNonNull(channel, "channel");
		this.keyPair = Objects.requireNonNull(keyPair, "keyPair");
		this.registration = Objects.requireNonNull(registration, "registration");
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
			// Own the channel end to end: strip everything vanilla installed. Our framing handlers
			// keep the wire vanilla-shaped.
			ChannelPipeline pipeline = channel.pipeline();
			pipeline.toMap().forEach((name, handler) -> pipeline.remove(handler));
			pipeline.addLast(VanillaFrameDecoder.HANDLER_NAME, new VanillaFrameDecoder(NEGOTIATION_MAX_FRAME_BYTES));
			pipeline.addLast(RESPONSE_HANDLER_NAME, new ResponseHandler());
			pipeline.addLast(VanillaFrameEncoder.HANDLER_NAME, new VanillaFrameEncoder());
			channel.config().setAutoRead(true);
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
			state = State.TAKEOVER_WRITE;
			beginTakeover(secret);
		} catch (IOException exception) {
			fail(HolepunchFailure.Kind.UNEXPECTED_PACKET, "invalid login encryption response", exception);
		} catch (GeneralSecurityException exception) {
			// A secret or challenge we cannot decrypt is the client's doing, not a server failure.
			fail(HolepunchFailure.Kind.UNEXPECTED_PACKET, "client login encryption response failed to decrypt", exception);
		}
	}

	private void beginTakeover(byte[] secret) {
		try {
			NettyEncryption encryption = new NettyEncryption(secret);
			connection = new NettyHolepunchConnection(channel, encryption, registration.maximumPendingWriteBytes(), registration.maximumFrameBytes());
			channel.closeFuture().addListener(ignored -> connection.closeWith(new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "Minecraft channel closed", null)));
			// Vanilla encrypts the whole framed packet after the key exchange; so do we: the
			// frame encoder prefixes the payload and the cipher encoder encrypts the frame.
			ChannelPipeline pipeline = channel.pipeline();
			pipeline.addBefore(VanillaFrameEncoder.HANDLER_NAME, CIPHER_ENCODER_NAME, new NettyCipherEncoder(encryption));
			ByteBuffer query = LoginCodec.takeoverQueryPacket(0);
			channel.writeAndFlush(Unpooled.wrappedBuffer(query)).addListener((ChannelFutureListener) future -> executeOnEventLoop(() -> {
				if (state != State.TAKEOVER_WRITE) {
					return;
				}
				if (!future.isSuccess()) {
					fail(HolepunchFailure.Kind.IO, "takeover query write failed", future.cause());
					return;
				}
				switchToTransport();
			}));
		} catch (Throwable throwable) {
			fail(HolepunchFailure.Kind.IO, "unable to activate holepunch connection", throwable);
		}
	}

	/** Swaps the negotiation framing for the coordinated transport and hands the channel to the application. */
	private void switchToTransport() {
		if (state != State.TAKEOVER_WRITE) {
			return;
		}
		try {
			ChannelPipeline pipeline = channel.pipeline();
			pipeline.remove(VanillaFrameDecoder.HANDLER_NAME);
			pipeline.remove(RESPONSE_HANDLER_NAME);
			pipeline.remove(VanillaFrameEncoder.HANDLER_NAME);
			pipeline.addLast(TRANSPORT_DECODER_NAME, new NettyTransportDecoder(connection.encryption(), connection, TransportFrame.MAX_FRAME_BYTES));
			pipeline.addLast(NettyHolepunchConnection.HANDLER_NAME, connection);
			registration.application().install(channel, connection);
			// The watchdog sits tail-ward of the application so an exception an application handler
			// does not handle also reaches it instead of dying at the pipeline tail with the channel
			// left open.
			pipeline.addLast(NettyHolepunchConnection.InboundWatchdog.class.getSimpleName(), new NettyHolepunchConnection.InboundWatchdog(connection));
			state = State.OPEN;
			DebugLog.log("SERVER", connection.debugId(), "application installed pipeline=%s", channel.pipeline().names());
		} catch (Throwable throwable) {
			fail(HolepunchFailure.Kind.IO, "unable to install the holepunch application", unwrap(throwable));
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
		HolepunchFailure failure = new HolepunchFailure(kind, message, cause);
		if (connection != null) {
			connection.closeWith(failure);
		} else {
			channel.close();
		}
		// Every path reaching here is pre-install: the application has no channel and no future, so
		// a server-side IO failure would vanish into a silent close, while client-caused outcomes
		// (timeouts, malformed responses, dropped sockets) are the client's to observe.
		if (kind == HolepunchFailure.Kind.IO) {
			DebugLog.hardError("SERVER", 0, message, failure);
		} else {
			DebugLog.failure("SERVER", 0, message, failure);
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
			if (cause instanceof DecoderException || cause instanceof IOException) {
				// A malformed login frame or a dropped socket is the client's doing, not a server failure.
				fail(HolepunchFailure.Kind.UNEXPECTED_PACKET, "login negotiation pipeline failed", cause);
				return;
			}
			fail(HolepunchFailure.Kind.IO, "login negotiation pipeline failed", cause);
		}
	}

	private enum State {
		NEW, REQUEST_SENT, TAKEOVER_WRITE, OPEN, CLOSED
	}
}
