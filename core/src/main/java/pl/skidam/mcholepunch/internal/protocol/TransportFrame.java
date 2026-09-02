package pl.skidam.mcholepunch.internal.protocol;

import java.util.Objects;

public record TransportFrame(Type type, byte[] payload) {
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
