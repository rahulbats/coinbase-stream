import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.ksql.avro_schemas.KsqlDataSourceSchema;
import objects.CoinbaseSnapshot;
import objects.NormalizedTransaction;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import serdes.*;
import sun.tools.jconsole.ProxyClient;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

public class JsonToAvroConvertor {
    public static void main(String args[]){
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "from-json-avro-sample");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put("schema.registry.url", "http://localhost:8081");
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);

        StreamsBuilder builder = new StreamsBuilder();
        Serde<CoinbaseSnapshot> snapshotSerde = Serdes.serdeFrom(new RawSerializer(), new RawDeserializer());
        KStream<String, CoinbaseSnapshot> snaps = builder.stream("coinbase_snapshot",  Consumed.with(Serdes.String(), snapshotSerde));

        //KStream<String, String> snaps = builder.stream("coinbase_snapshot",  Consumed.with(Serdes.String(), Serdes.String()));

        Schema schema = new Schema.Parser().parse("{\n" +
                "  \"name\": \"KsqlDataSourceSchema\",\n" +
                "  \"type\": \"record\",\n" +
                "  \"namespace\": \"io.confluent.ksql.avro_schemas\",\n" +
                "  \"fields\": [\n" +
                "    {\n" +
                "      \"name\": \"sequence\",\n" +
                "      \"type\": \"long\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"bids\",\n" +
                "      \"type\": {\n" +
                "        \"type\": \"array\",\n" +
                "        \"items\": {\n" +
                "          \"type\": \"array\",\n" +
                "          \"items\": \"string\"\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"asks\",\n" +
                "      \"type\": {\n" +
                "        \"type\": \"array\",\n" +
                "        \"items\": {\n" +
                "          \"type\": \"array\",\n" +
                "          \"items\": \"string\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}");


        Map<String, String> schemaProps = new HashMap<>();
        schemaProps.put("schema.registry.url", "http://localhost:8081");
        KafkaAvroSerializer serializer = new KafkaAvroSerializer();
        serializer.configure(schemaProps, false);

        KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
        deserializer.configure(schemaProps, false);

        Serde<Object> avroSerde = Serdes.serdeFrom(serializer, deserializer);
        snaps
                .filter(((key, value) -> value!=null))
                .mapValues(value -> {
                    InputStream input = new ByteArrayInputStream(value.toString().getBytes());
                    DataInputStream din = new DataInputStream(input);

                    Object datum = null;
                    try {
                    Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);

                    DatumReader<Object> reader = new GenericDatumReader<Object>(schema);


                        datum = reader.read(null, decoder);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return datum;
                })
                .to("from-json-to-avro", Produced.with(Serdes.String(), avroSerde));
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
