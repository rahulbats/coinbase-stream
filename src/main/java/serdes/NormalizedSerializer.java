package serdes;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import objects.CoinbaseSnapshot;
import objects.NormalizedTransaction;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class NormalizedSerializer implements Serializer<NormalizedTransaction> {
    private final ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, NormalizedTransaction data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }


    @Override
    public void close() {

    }
}
