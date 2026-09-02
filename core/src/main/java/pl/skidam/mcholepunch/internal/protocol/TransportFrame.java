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
		DATA((byte) 0),
		READY((byte) 1),
		ACK((byte) 2),
		COMMIT((byte) 3);

		private final byte wire;

		Type(byte wire) {
			this.wire = wire;
		}

		public byte wire() {
			return wire;
		}

		/** Returns the type carrying the given wire byte, or null when no type does. */
		public static Type of(int wire) {
			for (Type type : values()) {
				if (type.wire == wire) {
					return type;
				}
			}
			return null;
		}
	}
}
