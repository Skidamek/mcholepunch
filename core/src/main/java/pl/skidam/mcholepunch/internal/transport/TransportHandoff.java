package pl.skidam.mcholepunch.internal.transport;

/**
 * Coordinates the encrypted-to-raw transport handoff without knowing what the raw stream
 * contains. The state is shared by the NIO and Netty adapters so that the wire-ordering rules do
 * not drift between implementations. The exchange is a single COMMIT in each direction: an
 * endpoint flips to raw only after it has flushed its own COMMIT and received the peer's, so both
 * endpoints agree on the exact stream position where the raw era starts. Ordering does the rest:
 * bytes flushed before the local COMMIT surface in the peer's encrypted era, bytes after it in
 * the raw era.
 */
public final class TransportHandoff {
	private volatile boolean localCommitRequested;
	private boolean localCommitSent;
	private boolean peerCommitReceived;
	private volatile boolean raw;

	/** Whether the caller must send the COMMIT control frame; false when a commit is already in flight or the raw era has started. */
	public boolean requestCommit() {
		synchronized (this) {
			if (raw || localCommitRequested) {
				return false;
			}
			localCommitRequested = true;
			return true;
		}
	}

	/** Whether the raw era starts now, called after our COMMIT reached the wire. */
	public boolean onCommitSent() {
		synchronized (this) {
			if (raw) {
				return false;
			}
			localCommitSent = true;
			return activateWhenBothCommitted();
		}
	}

	/** Whether the raw era starts now, called when the peer's COMMIT arrived. */
	public boolean onPeerCommitReceived() {
		synchronized (this) {
			if (raw) {
				return false;
			}
			peerCommitReceived = true;
			return activateWhenBothCommitted();
		}
	}

	private boolean activateWhenBothCommitted() {
		if (localCommitSent && peerCommitReceived) {
			raw = true;
			return true;
		}
		return false;
	}

	/** Whether a commit is in flight: writes are rejected here because they can no longer land in either era correctly. */
	public boolean isCommitting() {
		return !raw && localCommitRequested;
	}

	public boolean isRaw() {
		return raw;
	}

	public Phase phase() {
		synchronized (this) {
			if (raw) return Phase.RAW;
			if (localCommitRequested || peerCommitReceived) return Phase.COMMITTING;
			return Phase.INITIAL;
		}
	}

	public enum Phase {
		INITIAL,
		COMMITTING,
		RAW
	}
}
