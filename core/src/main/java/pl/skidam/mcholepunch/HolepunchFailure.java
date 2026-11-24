package pl.skidam.mcholepunch;

import java.io.IOException;

public final class HolepunchFailure extends IOException {
	public enum Kind {
		AUTHENTICATION_REQUESTED, UNEXPECTED_PACKET, TIMEOUT, CLOSED, IO
	}

	private final Kind kind;

	public HolepunchFailure(Kind kind, String message) {
		super(message);
		this.kind = kind;
	}

	public HolepunchFailure(Kind kind, String message, Throwable cause) {
		super(message, cause);
		this.kind = kind;
	}

	public Kind kind() {
		return kind;
	}
}
