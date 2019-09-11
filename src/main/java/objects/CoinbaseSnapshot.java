package objects;

public class CoinbaseSnapshot {
    private long sequence;
    private String[][] bids = new String[50][3];
    private String[][] asks = new String[50][3];


    public long getSequence() {
        return sequence;
    }

    public void setSequence(long sequence) {
        this.sequence = sequence;
    }

    public String[][] getBids() {
        return bids;
    }

    public void setBids(String[][] bids) {
        this.bids = bids;
    }

    public String[][] getAsks() {
        return asks;
    }

    public void setAsks(String[][] asks) {
        this.asks = asks;
    }




}
