package pl.skidam.mcholepunch.internal.transport;

import java.util.List;

import pl.skidam.mcholepunch.internal.protocol.TransportFrame;

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
	private boolean localCommitRequested;
	private boolean localCommitSent;
	private boolean peerCommitReceived;
	private boolean raw;

	public Transition requestCommit() {
		synchronized (this) {
			if (raw || localCommitRequested) {
				return Transition.empty();
			}
			localCommitRequested = true;
			return new Transition(List.of(TransportFrame.Type.COMMIT), false);
		}
	}

	public Transition onLocalControlSent(TransportFrame.Type type) {
		synchronized (this) {
			if (raw || type != TransportFrame.Type.COMMIT) {
				return Transition.empty();
			}
			localCommitSent = true;
			return transition(List.of());
		}
	}

	public Transition onPeerControlReceived(TransportFrame.Type type) {
		synchronized (this) {
			if (raw || type != TransportFrame.Type.COMMIT) {
				return Transition.empty();
			}
			peerCommitReceived = true;
			return transition(List.of());
		}
	}

	/** Whether a commit is in flight: writes are rejected here because they can no longer land in either era correctly. */
	public boolean isCommitting() {
		synchronized (this) {
			return !raw && localCommitRequested;
		}
	}

	public boolean isRaw() {
		synchronized (this) {
			return raw;
		}
	}

	public Phase phase() {
		synchronized (this) {
			if (raw) return Phase.RAW;
			if (localCommitRequested || peerCommitReceived) return Phase.COMMITTING;
			return Phase.INITIAL;
		}
	}

	private Transition transition(List<TransportFrame.Type> outbound) {
		boolean activate = !raw && localCommitSent && peerCommitReceived;
		if (activate) {
			raw = true;
		}
		return new Transition(List.copyOf(outbound), activate);
	}

	public enum Phase {
		INITIAL,
		COMMITTING,
		RAW
	}

	public record Transition(List<TransportFrame.Type> outbound, boolean activatedRaw) {
		private static Transition empty() {
			return new Transition(List.of(), false);
		}
	}
}
