package pl.skidam.mcholepunch;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletionStage;

public interface HolepunchConnection extends AutoCloseable {
	/**
	 * Queues the remaining bytes without changing the supplied buffer's position. The caller must not mutate or reuse the buffer until the returned stage completes.
	 * <p>
	 * Bytes submitted while {@link #isRaw()} is {@code false} are carried by the encrypted transport and delivered to the peer through {@link HolepunchHandler#onRead(ByteBuffer)}; bytes
	 * submitted while it is {@code true} are carried by the raw stream and delivered through {@link HolepunchHandler#onRawRead(ByteBuffer)}. Applications that change their wire format at the
	 * handoff boundary must therefore not submit bytes between requesting the upgrade and this connection switching to raw.
	 */
	CompletionStage<Void> write(ByteBuffer data);

	/**
	 * Whether the handoff has completed and this connection now carries the caller-owned raw stream. Writes submitted while this is {@code true} reach the peer through
	 * {@link HolepunchHandler#onRawRead(ByteBuffer)}, earlier writes through {@link HolepunchHandler#onRead(ByteBuffer)}. The value never flips back.
	 */
	boolean isRaw();

	/**
	 * Starts the encrypted preparation phase. The stage completes after both endpoints have exchanged READY and ACK control frames. Application bytes must not be sent as the new raw stream until
	 * {@link #commitTransportUpgrade()} completes.
	 */
	CompletionStage<Void> prepareTransportUpgrade();

	/**
	 * Commits the prepared handoff. The COMMIT control frame is sent through the encrypted transport, and the stage completes only after both COMMIT frames have been flushed and received. The transport
	 * then carries the caller-owned raw stream without mcholepunch interpreting its contents.
	 */
	CompletionStage<Void> commitTransportUpgrade();

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
