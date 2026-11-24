package pl.skidam.mcholepunch.server.netty;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;

import pl.skidam.mcholepunch.internal.crypto.CipherStreamState;
import pl.skidam.mcholepunch.internal.crypto.MinecraftCrypt;
import pl.skidam.mcholepunch.internal.protocol.LoginCodec;
import pl.skidam.mcholepunch.internal.protocol.MinecraftFrameReader;
import pl.skidam.mcholepunch.internal.protocol.TakeoverQuery;
import pl.skidam.mcholepunch.internal.protocol.VarInts;

class NettyLoginNegotiatorTest {

	@Test
	void stripsVanillaPipelineNegotiatesAndHandsOverTheChannel() throws Exception {
		EmbeddedChannel channel = vanillaPipeline();
		KeyPair keyPair = keyPair();
		AtomicBoolean installed = new AtomicBoolean();
		try (NettyChannelRegistry.Registration registration = NettyChannelRegistry.register(1024, (channel0, connection) -> {
			channel0.pipeline().addLast("application", new ChannelInboundHandlerAdapter());
			installed.set(true);
		})) {
			NettyLoginNegotiator negotiator = new NettyLoginNegotiator(channel, keyPair, registration);

			negotiator.start();
			assertVanillaHandlersGone(channel);

			ByteBuf framedRequest = channel.readOutbound();
			assertNotNull(framedRequest);
			LoginCodec.EncryptionRequest request;
			try {
				request = LoginCodec.readEncryptionRequest(singleFrame(framedRequest));
			} finally {
				framedRequest.release();
			}
			assertFalse(installed.get());

			byte[] secret = MinecraftCrypt.newSecret();
			ByteBuffer response = unframedEncryptionResponse(MinecraftCrypt.rsaEncrypt(keyPair.getPublic(), secret), MinecraftCrypt.rsaEncrypt(keyPair.getPublic(), request.verifyToken()));
			channel.writeInbound(frame(response));

			ByteBuf encryptedQuery = channel.readOutbound();
			assertNotNull(encryptedQuery);
			ByteBuffer queryPayload;
			try {
				// The takeover query is encrypted including its frame prefix, matching the client
				// which decrypts the stream first and frames afterwards.
				byte[] encryptedBytes = new byte[encryptedQuery.readableBytes()];
				encryptedQuery.readBytes(encryptedBytes);
				CipherStreamState decryptor = MinecraftCrypt.decryptor(secret);
				ByteBuffer decryptedFrame = singleFrame(ByteBuffer.wrap(decryptor.update(encryptedBytes)));
				byte[] payload = new byte[decryptedFrame.remaining()];
				decryptedFrame.get(payload);
				queryPayload = ByteBuffer.wrap(payload);
			} finally {
				encryptedQuery.release();
			}
			TakeoverQuery query = LoginCodec.readTakeoverQuery(queryPayload);
			assertEquals(TakeoverQuery.IDENTIFIER, query.identifier());
			assertTrue(installed.get());

			List<String> names = channel.pipeline().names();
			int framer = names.indexOf(NettyHolepunchConnection.HANDLER_NAME);
			int application = names.indexOf("application");
			int watchdog = names.indexOf(NettyHolepunchConnection.InboundWatchdog.class.getSimpleName());
			assertTrue(framer < application && application < watchdog, "the application handlers sit tail-ward of the transport");
			assertEquals(names.size() - 2, watchdog, "the watchdog must be the last real handler so it also catches unhandled application exceptions");
			assertTrue(names.indexOf(NettyLoginNegotiator.CIPHER_ENCODER_NAME) < names.indexOf(NettyHolepunchConnection.HANDLER_NAME),
					"the framer must sit tail-ward of the cipher encoder so frames are written before encryption");
			assertNull(channel.pipeline().get(VanillaFrameDecoder.HANDLER_NAME));
		} finally {
			channel.finishAndReleaseAll();
		}
	}

	@Test
	void malformedResponseClosesWithoutInstalling() throws Exception {
		EmbeddedChannel channel = vanillaPipeline();
		AtomicBoolean installed = new AtomicBoolean();
		try (NettyChannelRegistry.Registration registration = NettyChannelRegistry.register(1024, (channel0, connection) -> installed.set(true))) {
			NettyLoginNegotiator negotiator = new NettyLoginNegotiator(channel, keyPair(), registration);

			negotiator.start();
			ByteBuf request = channel.readOutbound();
			request.release();
			channel.writeInbound(frame(ByteBuffer.wrap(new byte[]{2})));

			assertFalse(channel.isOpen());
			assertFalse(installed.get());
		} finally {
			channel.finishAndReleaseAll();
		}
	}

	@Test
	void failedTakeoverQueryWriteClosesWithoutInstalling() throws Exception {
		EmbeddedChannel channel = vanillaPipeline();
		KeyPair keyPair = keyPair();
		AtomicBoolean installed = new AtomicBoolean();
		try (NettyChannelRegistry.Registration registration = NettyChannelRegistry.register(1024, (channel0, connection) -> installed.set(true))) {
			NettyLoginNegotiator negotiator = new NettyLoginNegotiator(channel, keyPair, registration);

			negotiator.start();
			ByteBuf framedRequest = channel.readOutbound();
			LoginCodec.EncryptionRequest request;
			try {
				request = LoginCodec.readEncryptionRequest(singleFrame(framedRequest));
			} finally {
				framedRequest.release();
			}
			DeferredWriteHandler deferred = new DeferredWriteHandler();
			channel.pipeline().addLast("deferred-write", deferred);

			byte[] secret = MinecraftCrypt.newSecret();
			ByteBuffer response = unframedEncryptionResponse(MinecraftCrypt.rsaEncrypt(keyPair.getPublic(), secret), MinecraftCrypt.rsaEncrypt(keyPair.getPublic(), request.verifyToken()));
			channel.writeInbound(frame(response));

			deferred.promise.setFailure(new IllegalStateException("write failed"));

			assertFalse(installed.get());
			assertFalse(channel.isOpen());
			assertNull(channel.pipeline().get(NettyHolepunchConnection.HANDLER_NAME));
		} finally {
			channel.finishAndReleaseAll();
		}
	}

	private static void assertVanillaHandlersGone(EmbeddedChannel channel) {
		assertNull(channel.pipeline().get("splitter"));
		assertNull(channel.pipeline().get("decoder"));
		assertNull(channel.pipeline().get("prepender"));
		assertNull(channel.pipeline().get("packet_handler"));
		assertNotNull(channel.pipeline().get(VanillaFrameDecoder.HANDLER_NAME));
	}

	private static EmbeddedChannel vanillaPipeline() {
		EmbeddedChannel channel = new EmbeddedChannel();
		channel.pipeline().addLast("splitter", new ChannelInboundHandlerAdapter());
		channel.pipeline().addLast("decoder", new ChannelInboundHandlerAdapter());
		channel.pipeline().addLast("prepender", new ChannelOutboundHandlerAdapter());
		channel.pipeline().addLast("packet_handler", new ChannelInboundHandlerAdapter());
		return channel;
	}

	private static KeyPair keyPair() throws Exception {
		KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
		generator.initialize(2048);
		return generator.generateKeyPair();
	}

	private static ByteBuffer singleFrame(ByteBuf framed) throws Exception {
		byte[] bytes = new byte[framed.readableBytes()];
		framed.readBytes(bytes);
		return singleFrame(ByteBuffer.wrap(bytes));
	}

	private static ByteBuffer singleFrame(ByteBuffer framed) throws Exception {
		List<ByteBuffer> frames = new MinecraftFrameReader(2048).feed(framed, 1);
		assertEquals(1, frames.size());
		return frames.get(0);
	}

	private static ByteBuf frame(ByteBuffer payload) {
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		VarInts.write(output, payload.remaining());
		byte[] bytes = new byte[payload.remaining()];
		payload.duplicate().get(bytes);
		output.writeBytes(bytes);
		return Unpooled.wrappedBuffer(output.toByteArray());
	}

	private static ByteBuffer unframedEncryptionResponse(byte[] encryptedSecret, byte[] encryptedChallenge) {
		ByteArrayOutputStream packet = new ByteArrayOutputStream();
		VarInts.write(packet, 1);
		writeByteArray(packet, encryptedSecret);
		writeByteArray(packet, encryptedChallenge);
		return ByteBuffer.wrap(packet.toByteArray());
	}

	private static void writeByteArray(ByteArrayOutputStream output, byte[] value) {
		VarInts.write(output, value.length);
		output.writeBytes(value);
	}

	private static final class DeferredWriteHandler extends ChannelOutboundHandlerAdapter {
		private ChannelPromise promise;

		@Override
		public void write(ChannelHandlerContext context, Object message, ChannelPromise promise) {
			ReferenceCountUtil.release(message);
			this.promise = promise;
		}
	}
}
