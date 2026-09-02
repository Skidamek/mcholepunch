package pl.skidam.mcholepunch.internal.transport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import pl.skidam.mcholepunch.internal.crypto.TransportEncryption;
import pl.skidam.mcholepunch.internal.protocol.TransportFrame;
import pl.skidam.mcholepunch.internal.protocol.TransportFrameReader;

/**
 * Decrypts the encrypted transport and reassembles its frames. Shared by the NIO client loop and
 * the Netty server decoder so the wire-ordering rules cannot drift between the two adapters. Once
 * the raw switch has happened, {@link #feed} consumes nothing and the caller passes the remaining
 * bytes through untouched.
 */
public final class TransportFeed {
	private final TransportEncryption encryption;
	private final TransportFrameReader reader;
	private ByteBuffer plaintext = ByteBuffer.allocate(0);

	public TransportFeed(TransportEncryption encryption, TransportFrameReader reader) {
		this.encryption = encryption;
		this.reader = reader;
	}

	public boolean isRaw() {
		return encryption.isRaw();
	}

	/** Decrypts and frames what the encrypted transport owns, advancing the buffer position past the consumed bytes. */
	public List<TransportFrame> feed(ByteBuffer encryptedInput) throws IOException {
		List<TransportFrame> frames = new ArrayList<>();
		while (!encryption.isRaw() && encryptedInput.hasRemaining()) {
			int encryptedBytes = reader.maximumInputBytes(encryptedInput.remaining());
			if (plaintext.capacity() < encryptedBytes) {
				plaintext = ByteBuffer.allocate(encryptedBytes);
			}
			ByteBuffer encrypted = encryptedInput.slice();
			encrypted.limit(encryptedBytes);
			plaintext.clear();
			plaintext.limit(encryptedBytes);
			int plaintextBytes = encryption.decrypt(encrypted, plaintext);
			encryptedInput.position(encryptedInput.position() + encryptedBytes);
			if (plaintextBytes != encryptedBytes) {
				throw new IOException("transport cipher changed length from " + encryptedBytes + " to " + plaintextBytes);
			}
			plaintext.flip();
			frames.addAll(reader.feed(plaintext));
		}
		return frames;
	}
}
