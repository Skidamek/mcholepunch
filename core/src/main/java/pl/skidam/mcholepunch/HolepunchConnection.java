package pl.skidam.mcholepunch;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletionStage;

public interface HolepunchConnection extends AutoCloseable {
	/**
	 * Queues the remaining bytes without changing the supplied buffer's position. The caller must not mutate or reuse the buffer until the returned stage completes.
	 * <p>
	 * Bytes submitted while {@link #isRaw()} is {@code false} are carried by the encrypted transport and delivered to the peer through {@link HolepunchHandler#onRead(ByteBuffer)}; bytes
	 * submitted while it is {@code true} are carried by the raw stream and delivered through {@link HolepunchHandler#onRawRead(ByteBuffer)}. Writes submitted while
	 * {@link #commitTransportUpgrade()} is in flight fail instead of being carried, so an application must hold its own output across the flip and submit it once the commit has completed.
	 * <p>
	 * Completion callbacks must not block: a callback that waits on the commit future or a sibling write can deadlock the close path, which completes futures on one thread. Aggregate with {@code CompletableFuture.allOf}
	 * outside the callback instead.
	 */
	CompletionStage<Void> write(ByteBuffer data);

	/**
	 * Whether the handoff has completed and this connection now carries the caller-owned raw stream. Writes submitted while this is {@code true} reach the peer through
	 * {@link HolepunchHandler#onRawRead(ByteBuffer)}, earlier writes through {@link HolepunchHandler#onRead(ByteBuffer)}. The value never flips back.
	 */
	boolean isRaw();

	/**
	 * Switches the stream to the caller-owned raw format. The COMMIT control frame travels the encrypted transport, and the stage completes only after both endpoints have flushed their
	 * COMMIT and received the peer's; both then flip at the same stream position. Writes submitted while the commit is in flight fail, bytes flushed before the COMMIT surface in the peer's
	 * encrypted era, and everything after it is delivered raw without mcholepunch interpreting its contents.
	 */
	CompletionStage<Void> commitTransportUpgrade();

	/**
	 * The negotiated transport keying material, 16 bytes, identical on both endpoints. A layered
	 * protocol can derive its own keys from it, for example to encrypt a wire-format transition,
	 * while the connection is still open. Treat the value as sensitive and never log or persist it;
	 * the returned array is a copy.
	 */
	byte[] transportSecret();

	/**
	 * Stops future application-byte callbacks. At most the callback already in progress can finish
	 * after this method returns. The operation is idempotent and does not close the connection.
	 */
	void pauseReads();

	/**
	 * Resumes application-byte callbacks after a previous {@link #pauseReads()} call. The operation
	 * is idempotent and preserves callback ordering.
	 */
	void resumeReads();

	@Override
	void close();
}
