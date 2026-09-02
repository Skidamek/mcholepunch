package pl.skidam.mcholepunch.internal.protocol;

import java.util.Objects;

public record TransportFrame(Type type, byte[] payload) {
	/**
	 * Inbound frame cap. Receipt: a data frame is one write, and writes are bounded by the
	 * pending-write budget, whose default this matches. Remeasure both together if the default
	 * budget ever grows.
	 */
	public static final int MAX_FRAME_BYTES = 4 * 1024 * 1024;

	public TransportFrame {
		Objects.requireNonNull(type, "type");
		Objects.requireNonNull(payload, "payload");
	}

	public enum Type {
		DATA,
		READY,
		ACK,
		COMMIT
	}
}
