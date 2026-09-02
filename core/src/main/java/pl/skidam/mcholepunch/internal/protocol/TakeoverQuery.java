package pl.skidam.mcholepunch.internal.protocol;

public record TakeoverQuery(int transactionId, String identifier, byte[] payload) {
	public static final String IDENTIFIER = "mcholepunch:takeover";

	public TakeoverQuery {
		payload = payload.clone();
	}

	@Override
	public byte[] payload() {
		return payload.clone();
	}
}
