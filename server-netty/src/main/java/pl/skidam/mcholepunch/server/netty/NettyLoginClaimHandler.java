package pl.skidam.mcholepunch.server.netty;

import java.security.KeyPair;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.util.ReferenceCountUtil;

import pl.skidam.mcholepunch.internal.DebugLog;
import pl.skidam.mcholepunch.internal.protocol.HolepunchMarker;
import pl.skidam.mcholepunch.internal.protocol.LoginCodec;

/**
 * Claims connections whose Login Start carries a holepunch marker before vanilla builds a login
 * listener for them, so vanilla never owns (ticks, disconnects, or logs about) a holepunch
 * connection. Everything that is not a holepunch login is replayed untouched and this handler
 * removes itself.
 */
public final class NettyLoginClaimHandler extends ChannelInboundHandlerAdapter {
	/** The vanilla inbound frame decoder: the one pipeline position every accepted server channel has. */
	private static final String VANILLA_INBOUND_ANCHOR = "splitter";
	private static final String HANDLER_NAME = "mcholepunch-login-claim";

	private final KeyPair keyPair;
	private final byte[] markerConstant;
	private final boolean shouldAuthenticate;
	private ByteBuf handshake;
	private ScheduledFuture<?> heldHandshakeExpiry;

	/** Installs the claim handler after the vanilla frame decoder, before vanilla can own the login. */
	public static void install(ChannelPipeline pipeline, KeyPair keyPair) {
		pipeline.addAfter(VANILLA_INBOUND_ANCHOR, HANDLER_NAME, new NettyLoginClaimHandler(keyPair));
	}

	/**
	 * Installs the claim handler for a deployment that replaces the built-in 80-bit marker
	 * constant; the client must be configured with the same value or its logins are replayed to
	 * vanilla instead of claimed.
	 */
	public static void install(ChannelPipeline pipeline, KeyPair keyPair, byte[] markerConstant) {
		pipeline.addAfter(VANILLA_INBOUND_ANCHOR, HANDLER_NAME, new NettyLoginClaimHandler(keyPair, markerConstant));
	}

	/**
	 * Installs the claim handler with a custom 80-bit marker constant and the server's
	 * {@code shouldAuthenticate} flag for the Encryption Request packet, so claimed logins match
	 * the server's online-mode wire shape.
	 */
	public static void install(ChannelPipeline pipeline, KeyPair keyPair, byte[] markerConstant, boolean shouldAuthenticate) {
		pipeline.addAfter(VANILLA_INBOUND_ANCHOR, HANDLER_NAME, new NettyLoginClaimHandler(keyPair, markerConstant, shouldAuthenticate));
	}

	public NettyLoginClaimHandler(KeyPair keyPair) {
		this(keyPair, HolepunchMarker.defaultMarkerConstant(), true);
	}

	public NettyLoginClaimHandler(KeyPair keyPair, byte[] markerConstant) {
		this(keyPair, markerConstant, true);
	}

	public NettyLoginClaimHandler(KeyPair keyPair, byte[] markerConstant, boolean shouldAuthenticate) {
		this.keyPair = Objects.requireNonNull(keyPair, "keyPair");
		this.markerConstant = HolepunchMarker.requireMarkerConstant(markerConstant);
		this.shouldAuthenticate = shouldAuthenticate;
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (!(msg instanceof ByteBuf frame)) {
			forwardAndRemove(ctx, msg);
			return;
		}

		if (handshake == null) {
			if (!LoginCodec.isLoginIntention(frame.nioBuffer())) {
				forwardAndRemove(ctx, frame);
				return;
			}
			handshake = frame;
			// Vanilla owns no listener for a withheld handshake, so nothing ticks this channel: without
			// the fuse a claimant that stalls after its Login Intention pins the socket forever. A
			// marker client answers with its Login Start immediately, so the same one-step budget the
			// negotiator grants the encryption response is generous past any honest round trip.
			heldHandshakeExpiry = ctx.executor().schedule(() -> {
				ctx.close();
			}, NettyLoginNegotiator.DEFAULT_RESPONSE_TIMEOUT.toNanos(), TimeUnit.NANOSECONDS);
			return;
		}

		NettyChannelRegistry.Registration registration = NettyChannelRegistry.current();
		LoginCodec.LoginStart loginStart = readLoginStart(frame);
		Optional<HolepunchMarker> marker = loginStart == null ? Optional.empty() : markerOf(loginStart);
		if (registration == null || marker.isEmpty()) {
			replayAndRemove(ctx, frame);
			return;
		}

		ReferenceCountUtil.release(frame);
		releaseHeld();
		ctx.pipeline().remove(this);
		DebugLog.log("login takeover", 0, "marker accepted name=%s remote=%s", loginStart.username(), ctx.channel().remoteAddress());
		new NettyLoginNegotiator(ctx.channel(), keyPair, registration, shouldAuthenticate).start();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		if (handshake != null) {
			// No vanilla listener exists yet to turn this into a disconnect, and handing a reason to
			// a listener-less Connection would crash its teardown; drop the connection quietly.
			ctx.close();
			return;
		}
		super.exceptionCaught(ctx, cause);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		releaseHeld();
		super.channelInactive(ctx);
	}

	@Override
	public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
		cancelHeldHandshakeExpiry();
		super.handlerRemoved(ctx);
	}

	private void cancelHeldHandshakeExpiry() {
		ScheduledFuture<?> expiry = heldHandshakeExpiry;
		if (expiry != null) expiry.cancel(false);
	}

	private void forwardAndRemove(ChannelHandlerContext ctx, Object msg) {
		ByteBuf held = handshake;
		handshake = null;
		if (held != null) ctx.fireChannelRead(held);
		ctx.fireChannelRead(msg);
		ctx.pipeline().remove(this);
	}

	private void replayAndRemove(ChannelHandlerContext ctx, ByteBuf loginStartFrame) {
		ByteBuf held = handshake;
		handshake = null;
		ctx.fireChannelRead(held);
		ctx.fireChannelRead(loginStartFrame);
		ctx.pipeline().remove(this);
	}

	private void releaseHeld() {
		if (handshake != null) {
			ReferenceCountUtil.release(handshake);
			handshake = null;
		}
	}

	private LoginCodec.LoginStart readLoginStart(ByteBuf frame) {
		try {
			return LoginCodec.readLoginStart(frame.nioBuffer());
		} catch (Exception exception) {
			return null;
		}
	}

	private Optional<HolepunchMarker> markerOf(LoginCodec.LoginStart loginStart) {
		return HolepunchMarker.decode(markerConstant, loginStart.profileId());
	}
}
