package pl.skidam.mcholepunch.internal.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class TransportHandoffTest {
	@Test
	void flipsOnlyAfterBothCommitFramesCross() {
		TransportHandoff handoff = new TransportHandoff();

		assertTrue(handoff.requestCommit());
		assertTrue(handoff.isCommitting());
		assertFalse(handoff.onCommitSent());
		assertFalse(handoff.isRaw());
		assertTrue(handoff.onPeerCommitReceived());
		assertTrue(handoff.isRaw());
		assertFalse(handoff.isCommitting());
	}

	@Test
	void peerCommitAloneDoesNotFlipUntilLocalCommits() {
		TransportHandoff handoff = new TransportHandoff();

		assertFalse(handoff.onPeerCommitReceived());
		assertFalse(handoff.isCommitting(), "the peer's intent must not block local writes");

		assertTrue(handoff.requestCommit());
		assertTrue(handoff.onCommitSent());
		assertTrue(handoff.isRaw());
	}

	@Test
	void requestCommitIsIdempotent() {
		TransportHandoff handoff = new TransportHandoff();

		assertTrue(handoff.requestCommit());
		assertFalse(handoff.requestCommit());
		assertFalse(handoff.onCommitSent());
		assertFalse(handoff.requestCommit());
	}

	@Test
	void controlFramesAreIgnoredAfterTheFlip() {
		TransportHandoff handoff = new TransportHandoff();
		handoff.requestCommit();
		handoff.onCommitSent();
		assertTrue(handoff.onPeerCommitReceived());

		assertFalse(handoff.onPeerCommitReceived());
		assertFalse(handoff.requestCommit());
	}

	@Test
	void phasesProgressThroughTheCommit() {
		TransportHandoff handoff = new TransportHandoff();
		assertEquals(TransportHandoff.Phase.INITIAL, handoff.phase());

		handoff.requestCommit();
		assertEquals(TransportHandoff.Phase.COMMITTING, handoff.phase());

		handoff.onCommitSent();
		assertEquals(TransportHandoff.Phase.COMMITTING, handoff.phase());

		handoff.onPeerCommitReceived();
		assertEquals(TransportHandoff.Phase.RAW, handoff.phase());
	}
}
