package pl.skidam.mcholepunch.server.netty;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.junit.jupiter.api.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;

import pl.skidam.mcholepunch.internal.crypto.MinecraftCrypt;

class NettyEncryptionTest {
	@Test
	void encryptionIsContinuousAcrossWrites() throws Exception {
		byte[] secret = MinecraftCrypt.newSecret();
		NettyEncryption server = new NettyEncryption(secret);
		NettyEncryption client = new NettyEncryption(secret);
		EmbeddedChannel channel = new EmbeddedChannel(new NettyCipherEncoder(server));
		channel.writeOutbound(Unpooled.wrappedBuffer(new byte[]{1, 2, 3}));
		channel.writeOutbound(Unpooled.wrappedBuffer(new byte[]{4, 5}));
		byte[] first = readBytes(channel.readOutbound());
		byte[] second = readBytes(channel.readOutbound());
		assertArrayEquals(new byte[]{1, 2, 3}, client.decrypt(first));
		assertArrayEquals(new byte[]{4, 5}, client.decrypt(second));
		channel.finishAndReleaseAll();
	}

	@Test
	void byteBufCipherHandlesDirectAndCompositeBuffers() throws Exception {
		byte[] secret = MinecraftCrypt.newSecret();
		EmbeddedChannel encoder = new EmbeddedChannel(new NettyCipherEncoder(new NettyEncryption(secret)));
		ByteBuf direct = Unpooled.directBuffer(3).writeBytes(new byte[]{1, 2, 3});
		encoder.writeOutbound(direct);
		byte[] first = readBytes(encoder.readOutbound());
		CompositeByteBuf composite = Unpooled.compositeBuffer();
		composite.addComponents(true, Unpooled.wrappedBuffer(new byte[]{4}), Unpooled.wrappedBuffer(new byte[]{5, 6}));
		encoder.writeOutbound(composite);
		byte[] second = readBytes(encoder.readOutbound());
		NettyEncryption decryptor = new NettyEncryption(secret);
		assertArrayEquals(new byte[]{1, 2, 3}, decryptor.decrypt(first));
		assertArrayEquals(new byte[]{4, 5, 6}, decryptor.decrypt(second));
		EmbeddedChannel decoder = new EmbeddedChannel(new NettyCipherDecoder(new NettyEncryption(secret)));
		decoder.writeInbound(Unpooled.wrappedBuffer(first, second));
		assertArrayEquals(new byte[]{1, 2, 3, 4, 5, 6}, readBytes(decoder.readInbound()));
		encoder.finishAndReleaseAll();
		decoder.finishAndReleaseAll();
	}

	private static byte[] readBytes(ByteBuf buffer) {
		try {
			byte[] result = new byte[buffer.readableBytes()];
			buffer.readBytes(result);
			return result;
		} finally {
			buffer.release();
		}
	}
}
