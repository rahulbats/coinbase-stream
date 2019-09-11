package serdes;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import objects.CoinbaseSnapshot;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class RawSerializer implements Serializer<CoinbaseSnapshot> {
    private final ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, CoinbaseSnapshot data) {
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
