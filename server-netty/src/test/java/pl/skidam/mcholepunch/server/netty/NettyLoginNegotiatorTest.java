package pl.skidam.mcholepunch.server.netty;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.MessageToByteEncoder;

import pl.skidam.mcholepunch.HolepunchConnection;
import pl.skidam.mcholepunch.internal.crypto.CipherStreamState;
import pl.skidam.mcholepunch.internal.crypto.MinecraftCrypt;
import pl.skidam.mcholepunch.internal.protocol.HolepunchMarker;
import pl.skidam.mcholepunch.internal.protocol.LoginCodec;
import pl.skidam.mcholepunch.internal.protocol.MinecraftFrameReader;
import pl.skidam.mcholepunch.internal.protocol.TakeoverQuery;
import pl.skidam.mcholepunch.internal.protocol.VarInts;

class NettyLoginNegotiatorTest {

	@Test
	void negotiatesEncryptionAndHandsOverTheChannelOnlyAfterTakeoverWrite() throws Exception {
		EmbeddedChannel channel = minecraftPipeline();
		KeyPair keyPair = keyPair();
		AtomicBoolean installed = new AtomicBoolean();
		try (NettyChannelRegistry.Registration registration = NettyChannelRegistry.register(1024, (channel0, connection) -> {
			channel0.pipeline().addLast("application", new io.netty.channel.ChannelInboundHandlerAdapter());
			installed.set(true);
		})) {
			NettyTakeoverSpec spec = takeoverSpec();
			NettyLoginNegotiator negotiator = new NettyLoginNegotiator(channel, keyPair, "test", new InetSocketAddress("127.0.0.1", 25565), new HolepunchMarker(1, 0, 1), registration, spec);

			negotiator.start();
			ByteBuf requestBytes = channel.readOutbound();
			assertNotNull(requestBytes);
			LoginCodec.EncryptionRequest request;
			try {
				request = LoginCodec.readEncryptionRequest(singleFrame(requestBytes));
			} finally {
				requestBytes.release();
			}
			assertFalse(installed.get());

			byte[] secret = MinecraftCrypt.newSecret();
			ByteBuffer response = unframedEncryptionResponse(MinecraftCrypt.rsaEncrypt(keyPair.getPublic(), secret), MinecraftCrypt.rsaEncrypt(keyPair.getPublic(), request.verifyToken()));
			channel.pipeline().context("splitter").fireChannelRead(Unpooled.wrappedBuffer(response));

			ByteBuf encryptedQuery = channel.readOutbound();
			assertNotNull(encryptedQuery);
			ByteBuffer decrypted;
			try {
				CipherStreamState decryptor = MinecraftCrypt.decryptor(secret);
				byte[] bytes = new byte[encryptedQuery.readableBytes()];
				encryptedQuery.readBytes(bytes);
				decrypted = ByteBuffer.wrap(decryptor.update(bytes));
			} finally {
				encryptedQuery.release();
			}
			TakeoverQuery query = LoginCodec.readTakeoverQuery(singleFrame(decrypted));
			assertEquals(TakeoverQuery.IDENTIFIER, query.identifier());
			assertTrue(installed.get());
			assertNull(channel.pipeline().get(spec.negotiatorName()));
			assertNull(channel.pipeline().get("splitter"));
			assertNotNull(channel.pipeline().get(NettyHolepunchConnection.HANDLER_NAME));
			assertNotNull(channel.pipeline().get(spec.inboundWatchdogName()));
		} finally {
			channel.finishAndReleaseAll();
		}
	}

	@Test
	void malformedResponseClosesWithoutInstalling() throws Exception {
		EmbeddedChannel channel = minecraftPipeline();
		AtomicBoolean installed = new AtomicBoolean();
		try (NettyChannelRegistry.Registration registration = NettyChannelRegistry.register(1024, (channel0, connection) -> installed.set(true))) {
			NettyLoginNegotiator negotiator = new NettyLoginNegotiator(channel, keyPair(), "test", new InetSocketAddress("127.0.0.1", 25565), new HolepunchMarker(1, 0, 1), registration, takeoverSpec());

			negotiator.start();
			ByteBuf request = channel.readOutbound();
			request.release();
			channel.pipeline().context("splitter").fireChannelRead(Unpooled.wrappedBuffer(new byte[]{2}));

			assertFalse(channel.isOpen());
			assertFalse(installed.get());
		} finally {
			channel.finishAndReleaseAll();
		}
	}

	private static EmbeddedChannel minecraftPipeline() {
		EmbeddedChannel channel = new EmbeddedChannel();
		channel.pipeline().addLast("splitter", new io.netty.channel.ChannelInboundHandlerAdapter());
		channel.pipeline().addLast("flow_control", new io.netty.channel.ChannelInboundHandlerAdapter());
		channel.pipeline().addLast("decoder", new io.netty.channel.ChannelInboundHandlerAdapter());
		channel.pipeline().addLast("prepender", new MinecraftPrepender());
		channel.pipeline().addLast("encoder", new ChannelOutboundHandlerAdapter());
		channel.pipeline().addLast("packet_handler", new io.netty.channel.ChannelInboundHandlerAdapter());
		return channel;
	}

	private static NettyTakeoverSpec takeoverSpec() {
		return new NettyTakeoverSpec("splitter", "prepender", java.util.List.of("splitter", "flow_control", "decoder", "prepender", "encoder", "packet_handler"));
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

	private static final class MinecraftPrepender extends MessageToByteEncoder<ByteBuf> {
		@Override
		protected void encode(ChannelHandlerContext context, ByteBuf input, ByteBuf output) {
			ByteArrayOutputStream length = new ByteArrayOutputStream();
			VarInts.write(length, input.readableBytes());
			output.writeBytes(length.toByteArray());
			output.writeBytes(input, input.readerIndex(), input.readableBytes());
		}
	}
}
