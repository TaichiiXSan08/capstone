public class Transaction {
    public int transactionId;
    public String source;
    public String item;
    public double amount;
    public long timestamp;

    public Transaction() {}

    public Transaction(int transactionId, String source, String item, double amount) {
        this.transactionId = transactionId;
        this.source = source;
        this.item = item;
        this.amount = amount;
        this.timestamp = System.currentTimeMillis();
    }
}