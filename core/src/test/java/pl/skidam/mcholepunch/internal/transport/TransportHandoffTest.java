package pl.skidam.mcholepunch.internal.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import pl.skidam.mcholepunch.internal.protocol.TransportFrame;

class TransportHandoffTest {
	@Test
	void flipsOnlyAfterBothCommitFramesCross() {
		TransportHandoff handoff = new TransportHandoff();

		assertEquals(List.of(TransportFrame.Type.COMMIT), handoff.requestCommit().outbound());
		assertTrue(handoff.isCommitting());
		assertFalse(handoff.onLocalControlSent(TransportFrame.Type.COMMIT).activatedRaw());
		assertFalse(handoff.isRaw());
		assertTrue(handoff.onPeerControlReceived(TransportFrame.Type.COMMIT).activatedRaw());
		assertTrue(handoff.isRaw());
		assertFalse(handoff.isCommitting());
	}

	@Test
	void peerCommitAloneDoesNotFlipUntilLocalCommits() {
		TransportHandoff handoff = new TransportHandoff();

		assertFalse(handoff.onPeerControlReceived(TransportFrame.Type.COMMIT).activatedRaw());
		assertFalse(handoff.isCommitting(), "the peer's intent must not block local writes");

		assertEquals(List.of(TransportFrame.Type.COMMIT), handoff.requestCommit().outbound());
		assertTrue(handoff.onLocalControlSent(TransportFrame.Type.COMMIT).activatedRaw());
		assertTrue(handoff.isRaw());
	}

	@Test
	void requestCommitIsIdempotent() {
		TransportHandoff handoff = new TransportHandoff();

		assertEquals(List.of(TransportFrame.Type.COMMIT), handoff.requestCommit().outbound());
		assertEquals(List.of(), handoff.requestCommit().outbound());
		assertTrue(handoff.onLocalControlSent(TransportFrame.Type.COMMIT).outbound().isEmpty());
		assertEquals(List.of(), handoff.requestCommit().outbound());
	}

	@Test
	void controlFramesAreIgnoredAfterTheFlip() {
		TransportHandoff handoff = new TransportHandoff();
		handoff.requestCommit();
		handoff.onLocalControlSent(TransportFrame.Type.COMMIT);
		assertTrue(handoff.onPeerControlReceived(TransportFrame.Type.COMMIT).activatedRaw());

		assertTrue(handoff.onPeerControlReceived(TransportFrame.Type.COMMIT).outbound().isEmpty());
		assertTrue(handoff.requestCommit().outbound().isEmpty());
	}

	@Test
	void phasesProgressThroughTheCommit() {
		TransportHandoff handoff = new TransportHandoff();
		assertEquals(TransportHandoff.Phase.INITIAL, handoff.phase());

		handoff.requestCommit();
		assertEquals(TransportHandoff.Phase.COMMITTING, handoff.phase());

		handoff.onLocalControlSent(TransportFrame.Type.COMMIT);
		assertEquals(TransportHandoff.Phase.COMMITTING, handoff.phase());

		handoff.onPeerControlReceived(TransportFrame.Type.COMMIT);
		assertEquals(TransportHandoff.Phase.RAW, handoff.phase());
	}
}
