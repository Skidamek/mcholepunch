package pl.skidam.mcholepunch.internal.transport;

import java.util.ArrayList;
import java.util.List;

import pl.skidam.mcholepunch.internal.protocol.TransportFrame;

/**
 * Coordinates the encrypted-to-raw transport handoff without knowing what the
 * raw stream contains. The state is shared by the NIO and Netty adapters so
 * that the wire-ordering rules do not drift between implementations.
 */
public final class TransportHandoff {
	private boolean localPrepareRequested;
	private boolean localReadySent;
	private boolean peerReadyReceived;
	private boolean localAckQueued;
	private boolean peerAckSent;
	private boolean peerAckReceived;
	private boolean localCommitRequested;
	private boolean localCommitSent;
	private boolean peerCommitReceived;
	private boolean raw;
	private boolean prepared;

	public Transition requestPrepare() {
		synchronized (this) {
			if (raw || localPrepareRequested) {
				return Transition.empty();
			}
			localPrepareRequested = true;
			return transition(List.of(TransportFrame.Type.READY), false, false);
		}
	}

	public Transition requestCommit() {
		synchronized (this) {
			if (raw || localCommitRequested) {
				return Transition.empty();
			}
			if (!prepared) {
				throw new IllegalStateException("transport handoff is not prepared");
			}
			localCommitRequested = true;
			return transition(List.of(TransportFrame.Type.COMMIT), false, false);
		}
	}

	public Transition onLocalControlSent(TransportFrame.Type type) {
		synchronized (this) {
			if (raw) {
				return Transition.empty();
			}
			switch (type) {
				case READY -> {
					if (localReadySent) {
						return Transition.empty();
					}
					if (!localPrepareRequested) {
						throw new IllegalStateException("local READY was not requested");
					}
					localReadySent = true;
				}
				case ACK -> {
					if (peerAckSent) {
						return Transition.empty();
					}
					if (!localAckQueued || !peerReadyReceived) {
						throw new IllegalStateException("local ACK was not requested");
					}
					peerAckSent = true;
				}
				case COMMIT -> {
					if (localCommitSent) {
						return Transition.empty();
					}
					if (!localCommitRequested) {
						throw new IllegalStateException("local COMMIT was not requested");
					}
					localCommitSent = true;
				}
				case DATA -> throw new IllegalArgumentException("DATA is not a handoff control frame");
			}
			List<TransportFrame.Type> outbound = new ArrayList<>(1);
			if (peerReadyReceived && localReadySent && !localAckQueued) {
				localAckQueued = true;
				outbound.add(TransportFrame.Type.ACK);
			}
			return transition(outbound, true, localCommitSent && peerCommitReceived);
		}
	}

	public Transition onPeerControlReceived(TransportFrame.Type type) {
		synchronized (this) {
			if (raw) {
				return Transition.empty();
			}
			switch (type) {
				case READY -> {
					if (peerReadyReceived) {
						return Transition.empty();
					}
					peerReadyReceived = true;
				}
				case ACK -> {
					if (peerAckReceived) {
						return Transition.empty();
					}
					if (!localPrepareRequested) {
						throw new IllegalStateException("peer ACK arrived before local preparation");
					}
					peerAckReceived = true;
				}
				case COMMIT -> {
					if (peerCommitReceived) {
						return Transition.empty();
					}
					if (!localPrepareRequested) {
						throw new IllegalStateException("peer COMMIT arrived before handoff preparation");
					}
					peerCommitReceived = true;
				}
				case DATA -> throw new IllegalArgumentException("DATA is not a handoff control frame");
			}

			List<TransportFrame.Type> outbound = new ArrayList<>(1);
			if (peerReadyReceived && localReadySent && !localAckQueued) {
				localAckQueued = true;
				outbound.add(TransportFrame.Type.ACK);
			}
			return transition(outbound, true, localCommitSent && peerCommitReceived);
		}
	}

	public boolean blocksData() {
		synchronized (this) {
			return !raw && localPrepareRequested;
		}
	}

	public boolean isRaw() {
		synchronized (this) {
			return raw;
		}
	}

	public boolean isPrepared() {
		synchronized (this) {
			return prepared;
		}
	}

	public Phase phase() {
		synchronized (this) {
			if (raw) return Phase.RAW;
			if (localCommitRequested || peerCommitReceived) return Phase.COMMITTING;
			if (prepared) return Phase.PREPARED;
			if (localPrepareRequested || peerReadyReceived) return Phase.PREPARING;
			return Phase.INITIAL;
		}
	}

	private Transition transition(List<TransportFrame.Type> outbound, boolean mayPrepare, boolean mayCommit) {
		boolean preparedNow = mayPrepare && !prepared && localReadySent && peerAckReceived;
		if (preparedNow) {
			prepared = true;
		}
		boolean activateRaw = mayCommit && !raw;
		if (activateRaw) {
			raw = true;
		}
		return new Transition(List.copyOf(outbound), preparedNow, activateRaw);
	}

	public enum Phase {
		INITIAL,
		PREPARING,
		PREPARED,
		COMMITTING,
		RAW
	}

	public record Transition(List<TransportFrame.Type> outbound, boolean prepared, boolean committed) {
		private static Transition empty() {
			return new Transition(List.of(), false, false);
		}

		public boolean activatesRaw() {
			return committed;
		}
	}
}
