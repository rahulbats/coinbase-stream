package serdes;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import objects.CoinbaseSnapshot;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class RawDeserializer implements Deserializer<CoinbaseSnapshot> {
    private ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public CoinbaseSnapshot deserialize(String topic, byte[] data) {
        TypeReference<CoinbaseSnapshot> typeRef
                = new TypeReference<CoinbaseSnapshot>() {};
        CoinbaseSnapshot snapshot = null;
        try {
            snapshot = objectMapper.readValue(new String(data, "UTF-8"), typeRef );
        } catch (IOException e) {
            e.printStackTrace();
        }
        return snapshot;
    }

    @Override
    public void close() {

    }
}
