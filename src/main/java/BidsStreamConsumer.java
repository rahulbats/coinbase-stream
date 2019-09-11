import objects.NormalizedTransaction;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import serdes.*;
import objects.CoinbaseSnapshot;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class BidsStreamConsumer {
    public static void main(String args[]){
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "cb-transaction-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        Serde<CoinbaseSnapshot> snapshotSerde = Serdes.serdeFrom(new RawSerializer(), new RawDeserializer());
        KStream<String, CoinbaseSnapshot> snaps = builder.stream("coinbase_snapshot", Consumed.with(Serdes.String(), snapshotSerde));
        KStream<String, NormalizedTransaction>[] transactionKStream = snaps.mapValues( bidsObject -> {
            //System.out.println(bidsObject);
            long sequence = bidsObject.getSequence();
            List<NormalizedTransaction> normalizedTransactions = Arrays.stream(bidsObject.getBids()).map(bid->{
                return new NormalizedTransaction(sequence, "bid",Double.parseDouble(bid[0]), Double.parseDouble(bid[1]), Integer.parseInt( bid[2]));
            }).collect(Collectors.toList());
            normalizedTransactions.addAll(
            Arrays.stream(bidsObject.getAsks()).map(bid->{
                return new NormalizedTransaction(sequence, "ask",Double.parseDouble(bid[0]), Double.parseDouble(bid[1]), Integer.parseInt( bid[2]));
            }).collect(Collectors.toList()));
            return normalizedTransactions;
        })//.through("coinbase-messages-with-array", Produced.with(Serdes.String(), Serdes.serdeFrom(new NormalizedListSerializer(), new NormalizedListDeserializer())))
            .flatMapValues(value->value)//.through("coinbase_messages-without-array", Produced.with(Serdes.String(), Serdes.serdeFrom(new NormalizedSerializer(), new NormalizedDeserializer())))
                .branch((key, value) -> value.getTransactionType().equals("bid"),(key, value) -> value.getTransactionType().equals("ask") );

        transactionKStream[0]
                .mapValues(value -> value.toString())
                    .to("coinbase_bids");

        transactionKStream[1]
                .mapValues(value -> value.toString())
                .to("coinbase_asks");

        KStream<String, NormalizedTransaction> keyed = transactionKStream[0].map(((key, value) -> new KeyValue<String, NormalizedTransaction>(""+value.getSequence(), value)));

        KTable<String, Long> tables = keyed.groupByKey().count();


        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                streams.close();
            }
        }));
    }
}
