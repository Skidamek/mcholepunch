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
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.MessageToByteEncoder;

import pl.skidam.mcholepunch.HolepunchConnection;
import pl.skidam.mcholepunch.HolepunchHandler;
import pl.skidam.mcholepunch.internal.crypto.CipherStreamState;
import pl.skidam.mcholepunch.internal.crypto.MinecraftCrypt;
import pl.skidam.mcholepunch.internal.protocol.HolepunchMarker;
import pl.skidam.mcholepunch.internal.protocol.LoginCodec;
import pl.skidam.mcholepunch.internal.protocol.MinecraftFrameReader;
import pl.skidam.mcholepunch.internal.protocol.TakeoverQuery;
import pl.skidam.mcholepunch.internal.protocol.VarInts;
import pl.skidam.mcholepunch.server.HolepunchServerRegistry;

class NettyLoginNegotiatorTest {

	@Test
	void negotiatesEncryptionAndOpensOnlyAfterTakeoverWrite() throws Exception {
		EmbeddedChannel channel = minecraftPipeline();
		KeyPair keyPair = keyPair();
		AtomicBoolean opened = new AtomicBoolean();
		try (HolepunchServerRegistry.Registration registration = HolepunchServerRegistry.register(Runnable::run, 1024, Duration.ofSeconds(1), (username, remote, marker) -> new HolepunchHandler() {
			@Override
			public void onOpen(HolepunchConnection connection) {
				opened.set(true);
			}
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
			assertFalse(opened.get());

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
			assertTrue(opened.get());
			assertNull(channel.pipeline().get(spec.negotiatorName()));
			assertNull(channel.pipeline().get("splitter"));
			assertNotNull(channel.pipeline().get(spec.rawInboundName()));
		} finally {
			channel.finishAndReleaseAll();
		}
	}

	@Test
	void malformedResponseClosesWithoutOpening() throws Exception {
		EmbeddedChannel channel = minecraftPipeline();
		AtomicBoolean opened = new AtomicBoolean();
		try (HolepunchServerRegistry.Registration registration = HolepunchServerRegistry.register(Runnable::run, 1024, Duration.ofSeconds(1), (username, remote, marker) -> new HolepunchHandler() {
			@Override
			public void onOpen(HolepunchConnection connection) {
				opened.set(true);
			}
		})) {
			NettyLoginNegotiator negotiator = new NettyLoginNegotiator(channel, keyPair(), "test", new InetSocketAddress("127.0.0.1", 25565), new HolepunchMarker(1, 0, 1), registration, takeoverSpec());

			negotiator.start();
			ByteBuf request = channel.readOutbound();
			request.release();
			channel.pipeline().context("splitter").fireChannelRead(Unpooled.wrappedBuffer(new byte[]{2}));

			assertFalse(channel.isOpen());
			assertFalse(opened.get());
		} finally {
			channel.finishAndReleaseAll();
		}
	}

	private static EmbeddedChannel minecraftPipeline() {
		EmbeddedChannel channel = new EmbeddedChannel();
		channel.pipeline().addLast("splitter", new ChannelInboundHandlerAdapter());
		channel.pipeline().addLast("flow_control", new ChannelInboundHandlerAdapter());
		channel.pipeline().addLast("decoder", new ChannelInboundHandlerAdapter());
		channel.pipeline().addLast("prepender", new MinecraftPrepender());
		channel.pipeline().addLast("encoder", new ChannelOutboundHandlerAdapter());
		channel.pipeline().addLast("packet_handler", new ChannelInboundHandlerAdapter());
		return channel;
	}

	private static NettyTakeoverSpec takeoverSpec() {
		return new NettyTakeoverSpec("splitter", "prepender", List.of("timeout", "splitter", "flow_control", "decoder", "prepender", "encoder", "hackfix", "packet_handler"));
	}

	private static KeyPair keyPair() throws Exception {
		KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
		generator.initialize(1024);
		return generator.generateKeyPair();
	}

	private static ByteBuffer singleFrame(ByteBuf framed) throws Exception {
		byte[] bytes = new byte[framed.readableBytes()];
		framed.getBytes(framed.readerIndex(), bytes);
		return singleFrame(ByteBuffer.wrap(bytes));
	}

	private static ByteBuffer singleFrame(ByteBuffer framed) throws Exception {
		List<ByteBuffer> frames = new MinecraftFrameReader(2048).feed(framed);
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
