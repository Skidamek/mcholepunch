package pl.skidam.mcholepunch.server.netty;

import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;

import io.netty.buffer.ByteBuf;

import pl.skidam.mcholepunch.internal.crypto.TransportEncryption;

public final class NettyEncryption {
	private final TransportEncryption encryption;
	private final byte[] transportSecret;

	public NettyEncryption(byte[] secret) throws GeneralSecurityException {
		encryption = TransportEncryption.server(secret);
		transportSecret = secret.clone();
	}

	public void decrypt(ByteBuf input, ByteBuf output) {
		update(encryption, false, input, output);
	}

	public void encrypt(ByteBuf input, ByteBuf output) {
		update(encryption, true, input, output);
	}

	public TransportEncryption transport() {
		return encryption;
	}

	public byte[] transportSecret() {
		return transportSecret.clone();
	}

	public boolean isRaw() {
		return encryption.isRaw();
	}

	public void activateRaw() {
		encryption.activateRaw();
	}

	private static void update(TransportEncryption encryption, boolean encrypt, ByteBuf input, ByteBuf output) {
		int readableBytes = input.readableBytes();
		if (readableBytes == 0) {
			return;
		}
		output.ensureWritable(readableBytes);
		int writerIndex = output.writerIndex();
		if (input.nioBufferCount() == 1) {
			writerIndex += update(encryption, encrypt, input.nioBuffer(input.readerIndex(), readableBytes), output.nioBuffer(writerIndex, readableBytes), readableBytes);
		} else {
			for (ByteBuffer source : input.nioBuffers(input.readerIndex(), readableBytes)) {
				int sourceBytes = source.remaining();
				writerIndex += update(encryption, encrypt, source, output.nioBuffer(writerIndex, sourceBytes), sourceBytes);
			}
		}
		output.writerIndex(writerIndex);
	}

	private static int update(TransportEncryption encryption, boolean encrypt, ByteBuffer input, ByteBuffer output, int expectedBytes) {
		int written = encrypt ? encryption.encrypt(input, output) : encryption.decrypt(input, output);
		if (written != expectedBytes) {
			throw new IllegalStateException("transport cipher changed length from " + expectedBytes + " to " + written);
		}
		return written;
	}
}
