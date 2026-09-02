package pl.skidam.mcholepunch.internal.protocol;

public record TakeoverQuery(int transactionId, String identifier) {
	public static final String IDENTIFIER = "mcholepunch:takeover";
}
