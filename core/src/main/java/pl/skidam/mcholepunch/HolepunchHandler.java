package pl.skidam.mcholepunch;

import java.nio.ByteBuffer;

public interface HolepunchHandler {
	default void onOpen(HolepunchConnection connection) {}

	/**
	 * Receives a read-only, callback-scoped view of application bytes. The handler must consume or
	 * copy the bytes before returning. Store the connection from {@link #onOpen(HolepunchConnection)}
	 * and use its read-flow methods when the application cannot accept more data.
	 */
	default void onRead(ByteBuffer data) {}

	/**
	 * Receives application bytes carried by the raw post-handoff stream, delivered in stream order
	 * after {@link HolepunchConnection#commitTransportUpgrade()} has completed on this endpoint.
	 * Bytes delivered here were written by the peer after the peer switched to the raw stream, so a
	 * layered protocol that changes its wire format at the handoff boundary can rely on this
	 * callback to apply the post-handoff decoding. The default forwards to {@link #onRead(ByteBuffer)}
	 * for handlers that treat both eras identically.
	 */
	default void onRawRead(ByteBuffer data) {
		onRead(data);
	}

	default void onClosed(HolepunchFailure failure) {}
}
