package pl.skidam.mcholepunch;

import java.io.IOException;
import java.util.Objects;

public final class HolepunchFailure extends IOException {
	public enum Kind {
		UNEXPECTED_PACKET, TIMEOUT, CLOSED, IO
	}

	private final Kind kind;

	public HolepunchFailure(Kind kind, String message) {
		super(message);
		this.kind = Objects.requireNonNull(kind, "kind");
	}

	public HolepunchFailure(Kind kind, String message, Throwable cause) {
		super(message, cause);
		this.kind = Objects.requireNonNull(kind, "kind");
	}

	public Kind kind() {
		return kind;
	}
}
