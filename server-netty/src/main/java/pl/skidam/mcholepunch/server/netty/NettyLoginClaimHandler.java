package pl.skidam.mcholepunch.server.netty;

import java.security.KeyPair;
import java.util.Optional;

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
	private ByteBuf handshake;

	/** Installs the claim handler after the vanilla frame decoder, before vanilla can own the login. */
	public static void install(ChannelPipeline pipeline, KeyPair keyPair) {
		pipeline.addAfter(VANILLA_INBOUND_ANCHOR, HANDLER_NAME, new NettyLoginClaimHandler(keyPair));
	}

	public NettyLoginClaimHandler(KeyPair keyPair) {
		this.keyPair = keyPair;
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
		new NettyLoginNegotiator(ctx.channel(), keyPair, loginStart.username(), ctx.channel().remoteAddress(), marker.get(), registration).start();
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

	private static Optional<HolepunchMarker> markerOf(LoginCodec.LoginStart loginStart) {
		return HolepunchMarker.decode(loginStart.profileId());
	}
}
