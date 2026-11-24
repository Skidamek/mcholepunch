package pl.skidam.mcholepunch.internal.transport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

import pl.skidam.mcholepunch.internal.crypto.TransportEncryption;
import pl.skidam.mcholepunch.internal.protocol.TransportFrame;
import pl.skidam.mcholepunch.internal.protocol.TransportFrameReader;

/**
 * Decrypts the encrypted transport and reassembles its frames. Shared by the NIO client loop and
 * the Netty server decoder so the wire-ordering rules cannot drift between the two adapters. Each
 * frame is handed to {@code frames} the moment it completes, because a COMMIT may flip the era
 * mid-buffer: the peer starts its raw stream right after its own COMMIT, so that COMMIT and raw
 * bytes can share one TCP segment, and once the flip lands the rest of the segment belongs to the
 * raw stream untouched. Once the raw switch has happened, {@link #feed} consumes nothing and the
 * caller passes the remaining bytes through untouched.
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
	public void feed(ByteBuffer encryptedInput, Consumer<TransportFrame> frames) throws IOException {
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
			for (TransportFrame frame : reader.feed(plaintext)) {
				frames.accept(frame);
			}
		}
	}

	/**
	 * Frames bytes that are already decrypted, such as the leftovers of the takeover read: the
	 * takeover query is parsed out of a decrypted buffer, so whatever trailed it in the same TCP
	 * read must not go through the cipher a second time. Feeding it here keeps partial frames in
	 * the same reader state the encrypted era continues with.
	 */
	public void feedPlaintext(ByteBuffer plaintextInput, Consumer<TransportFrame> frames) throws IOException {
		for (TransportFrame frame : reader.feed(plaintextInput)) {
			frames.accept(frame);
		}
	}
}
