package pl.skidam.mcholepunch.server.netty;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;

import pl.skidam.mcholepunch.internal.protocol.HolepunchMarker;
import pl.skidam.mcholepunch.internal.protocol.LoginCodec;
import pl.skidam.mcholepunch.internal.protocol.MinecraftFrameReader;
import pl.skidam.mcholepunch.internal.protocol.VarInts;

/**
 * Drives the claim decision on an embedded pipeline: a decodable marker is claimed, anything else
 * is replayed untouched, and a withheld handshake cannot pin the channel past its expiry.
 */
class NettyLoginClaimHandlerTest {
	private static final byte[] MARKER_CONSTANT = {(byte) 0xA5, 0x5A, (byte) 0xC3, 0x3C, (byte) 0x96, 0x69, (byte) 0x7E, (byte) 0x87, 0x11, (byte) 0xEE};

	@Test
	void replaysLoginsThatCarryNoClaimableMarker() throws Exception {
		NettyChannelRegistry.Registration registration = NettyChannelRegistry.register(1024, (channel, connection) -> {});
		try {
			// A real account UUID is not a marker: the login must pass through untouched.
			EmbeddedChannel channel = claimChannel(null);
			List<byte[]> forwarded = forwardToRecorder(channel);
			channel.writeInbound(Unpooled.wrappedBuffer(inbound(LoginCodec.handshake(763, "server.test", 25565))));
			channel.writeInbound(Unpooled.wrappedBuffer(inbound(LoginCodec.loginStart("Notch", UUID.randomUUID()))));
			assertEquals(2, forwarded.size(), "the vanilla login must pass through untouched");
			assertTrue(LoginCodec.isLoginIntention(ByteBuffer.wrap(forwarded.get(0))), "the held handshake must be replayed first");
			assertEquals("Notch", LoginCodec.readLoginStart(ByteBuffer.wrap(forwarded.get(1))).username(), "the login start must be replayed second");
			assertNull(channel.pipeline().get(NettyLoginClaimHandler.class), "the claim handler must remove itself after a replay");
			assertNull(channel.readOutbound(), "no negotiation may start for a vanilla login");
			channel.finishAndReleaseAll();

			// A Login Start that does not even parse is replayed the same way.
			EmbeddedChannel malformed = claimChannel(null);
			List<byte[]> malformedForwarded = forwardToRecorder(malformed);
			malformed.writeInbound(Unpooled.wrappedBuffer(inbound(LoginCodec.handshake(763, "server.test", 25565))));
			malformed.writeInbound(Unpooled.wrappedBuffer(inbound(payloadWithoutUuid())));
			assertEquals(2, malformedForwarded.size(), "an unparseable Login Start must be replayed with its handshake");
			malformed.finishAndReleaseAll();
		} finally {
			registration.close();
		}
	}

	@Test
	void expiresAHeldHandshake() throws Exception {
		EmbeddedChannel channel = claimChannel(null);
		try {
			channel.writeInbound(Unpooled.wrappedBuffer(inbound(LoginCodec.handshake(763, "server.test", 25565))));
			assertTrue(channel.isOpen());

			channel.advanceTimeBy(NettyLoginNegotiator.DEFAULT_RESPONSE_TIMEOUT.toSeconds() + 1, TimeUnit.SECONDS);
			channel.runScheduledPendingTasks();

			assertFalse(channel.isOpen(), "a claimant that stalls after its Login Intention must not pin the channel");
		} finally {
			channel.finishAndReleaseAll();
		}
	}

	@Test
	void claimsAMarkerBuiltUnderTheInstalledConstant() throws Exception {
		// With nothing registered, even a decodable marker is replayed: the claim needs an
		// application to hand the channel to.
		EmbeddedChannel unregistered = claimChannel(null);
		List<byte[]> unregisteredForwarded = forwardToRecorder(unregistered);
		unregistered.writeInbound(Unpooled.wrappedBuffer(inbound(LoginCodec.handshake(763, "server.test", 25565))));
		unregistered.writeInbound(Unpooled.wrappedBuffer(inbound(LoginCodec.loginStart("Steve", HolepunchMarker.create(0, 777)))));
		assertEquals(2, unregisteredForwarded.size(), "a valid marker without a registration must be replayed");
		assertNull(unregistered.readOutbound());
		unregistered.finishAndReleaseAll();

		NettyChannelRegistry.Registration registration = NettyChannelRegistry.register(1024, (channel, connection) -> {});
		try {
			EmbeddedChannel channel = claimChannel(MARKER_CONSTANT);
			channel.writeInbound(Unpooled.wrappedBuffer(inbound(LoginCodec.handshake(763, "server.test", 25565))));
			channel.writeInbound(Unpooled.wrappedBuffer(inbound(LoginCodec.loginStart("Steve", HolepunchMarker.create(MARKER_CONSTANT, 0, 777)))));

			assertNull(channel.pipeline().get(NettyLoginClaimHandler.class), "the claim handler must remove itself once it claims");
			ByteBuf request = channel.readOutbound();
			assertNotNull(request, "the negotiator must answer the claim with an encryption request");
			MinecraftFrameReader reader = new MinecraftFrameReader(64 * 1024);
			List<ByteBuffer> frames = reader.feed(request.nioBuffer());
			assertEquals(1, frames.size(), "the encryption request must be one vanilla-shaped frame");
			assertFalse(LoginCodec.readEncryptionRequest(frames.get(0)).shouldAuthenticate(), "the negotiator must not request Mojang authentication");
			request.release();

			// The same login under the built-in constant must not be claimed by this deployment.
			EmbeddedChannel foreign = claimChannel(MARKER_CONSTANT);
			List<byte[]> forwarded = forwardToRecorder(foreign);
			foreign.writeInbound(Unpooled.wrappedBuffer(inbound(LoginCodec.handshake(763, "server.test", 25565))));
			foreign.writeInbound(Unpooled.wrappedBuffer(inbound(LoginCodec.loginStart("Eve", HolepunchMarker.create(0, 777)))));
			assertEquals(2, forwarded.size(), "a marker from another deployment must be replayed");
			foreign.finishAndReleaseAll();
		} finally {
			registration.close();
		}
	}

	private static EmbeddedChannel claimChannel(byte[] markerConstant) throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel();
		// The claim handler installs itself after the vanilla inbound frame decoder, whose pipeline
		// name it anchors on; a passthrough stands in for that decoder here.
		channel.pipeline().addFirst("splitter", new ChannelInboundHandlerAdapter());
		if (markerConstant == null) {
			NettyLoginClaimHandler.install(channel.pipeline(), keyPair());
		} else {
			NettyLoginClaimHandler.install(channel.pipeline(), keyPair(), markerConstant);
		}
		return channel;
	}

	private static List<byte[]> forwardToRecorder(EmbeddedChannel channel) {
		List<byte[]> forwarded = new ArrayList<>();
		channel.pipeline().addLast("recorder", new SimpleChannelInboundHandler<ByteBuf>() {
			@Override
			protected void channelRead0(ChannelHandlerContext context, ByteBuf frame) {
				byte[] bytes = new byte[frame.readableBytes()];
				frame.readBytes(bytes);
				forwarded.add(bytes);
			}
		});
		return forwarded;
	}

	private static KeyPair keyPair() throws Exception {
		KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
		generator.initialize(512);
		return generator.generateKeyPair();
	}

	/** Copies a framed packet, stripping the vanilla frame prefix: the claim handler sits behind the inbound frame decoder, so it sees packet payloads. */
	private static ByteBuffer inbound(ByteBuffer framedPacket) throws Exception {
		ByteBuffer view = framedPacket.duplicate();
		VarInts.read(view);
		byte[] bytes = new byte[view.remaining()];
		view.get(bytes);
		return ByteBuffer.wrap(bytes);
	}

	/** A Login Start-shaped payload that fails to parse: name present, profile id missing. */
	private static ByteBuffer payloadWithoutUuid() {
		return ByteBuffer.wrap(new byte[]{0x00, 0x01, 'x'});
	}
}
