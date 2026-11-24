package pl.skidam.mcholepunch.internal.protocol;

import java.util.Objects;

public record TransportFrame(Type type, byte[] payload) {
	/**
	 * Protocol ceiling for one transport frame, equal to the vanilla login frame cap. The reader
	 * accepts up to this; the writer splits at the per-connection send cap.
	 *
	 * A larger frame costs memory, not throughput: measured encrypted throughput is identical for
	 * 1 KB, 64 KB and 1 MB frames, while every frame allocates a payload array, a frame buffer and
	 * cipher output of its size, and large arrays land in G1 as humongous objects. The ceiling
	 * matches Minecraft's login decoder; the default send cap below is the sane value for callers
	 * that do not need to force a larger frame.
	 */
	public static final int MAX_FRAME_BYTES = LoginCodec.MAX_FRAME_BYTES;

	/** Default send-side frame cap; callers may raise it up to {@link #MAX_FRAME_BYTES}. */
	public static final int DEFAULT_MAX_FRAME_BYTES = 256 * 1024;

	public TransportFrame {
		Objects.requireNonNull(type, "type");
		Objects.requireNonNull(payload, "payload");
	}

	public enum Type {
		DATA((byte) 0),
		COMMIT((byte) 1);

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
