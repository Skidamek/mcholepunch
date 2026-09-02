package pl.skidam.mcholepunch.internal.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import pl.skidam.mcholepunch.internal.protocol.TransportFrame;

class TransportHandoffTest {
	@Test
	void keepsEncryptedDataFlowingUntilLocalPrepare() {
		TransportHandoff handoff = new TransportHandoff();

		assertEquals(List.of(), handoff.onPeerControlReceived(TransportFrame.Type.READY).outbound());
		assertFalse(handoff.blocksData());

		assertEquals(List.of(TransportFrame.Type.READY), handoff.requestPrepare().outbound());
		assertTrue(handoff.blocksData());
		assertEquals(List.of(TransportFrame.Type.ACK), handoff.onLocalControlSent(TransportFrame.Type.READY).outbound());
		assertFalse(handoff.isPrepared());
		assertTrue(handoff.onLocalControlSent(TransportFrame.Type.ACK).outbound().isEmpty());

		assertTrue(handoff.onPeerControlReceived(TransportFrame.Type.ACK).prepared());
		assertTrue(handoff.isPrepared());
	}

	@Test
	void commitRequiresBothCommitFrames() {
		TransportHandoff handoff = preparedHandoff();

		assertEquals(List.of(TransportFrame.Type.COMMIT), handoff.requestCommit().outbound());
		assertFalse(handoff.isRaw());
		assertFalse(handoff.onPeerControlReceived(TransportFrame.Type.COMMIT).committed());
		assertFalse(handoff.isRaw());
		assertTrue(handoff.onLocalControlSent(TransportFrame.Type.COMMIT).committed());
		assertTrue(handoff.isRaw());
		assertFalse(handoff.blocksData());
	}

	@Test
	void acceptsPeerCommitWhileLocalAdapterIsStillPreparing() {
		TransportHandoff handoff = preparedHandoff();
		// The peer may commit first while the local owner is still preparing its adapter.
		// The local COMMIT is still required before raw activation.
		handoff.onPeerControlReceived(TransportFrame.Type.COMMIT);
		handoff.requestCommit();
		handoff.onLocalControlSent(TransportFrame.Type.COMMIT);
		assertTrue(handoff.isRaw());
	}

	@Test
	void rejectsCommitBeforePreparation() {
		TransportHandoff handoff = new TransportHandoff();
		assertThrows(IllegalStateException.class, handoff::requestCommit);
		assertThrows(IllegalStateException.class, () -> handoff.onPeerControlReceived(TransportFrame.Type.ACK));
	}

	@Test
	void recordsPeerAckBeforeLocalWriteCallbackButWaitsForLocalReady() {
		TransportHandoff handoff = new TransportHandoff();
		handoff.requestPrepare();

		assertFalse(handoff.onPeerControlReceived(TransportFrame.Type.ACK).prepared());
		assertTrue(handoff.onLocalControlSent(TransportFrame.Type.READY).prepared());
	}

	private static TransportHandoff preparedHandoff() {
		TransportHandoff handoff = new TransportHandoff();
		handoff.requestPrepare();
		handoff.onLocalControlSent(TransportFrame.Type.READY);
		handoff.onPeerControlReceived(TransportFrame.Type.READY);
		handoff.onLocalControlSent(TransportFrame.Type.ACK);
		handoff.onPeerControlReceived(TransportFrame.Type.ACK);
		return handoff;
	}
}
