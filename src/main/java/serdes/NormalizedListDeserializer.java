package serdes;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import objects.NormalizedTransaction;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class NormalizedListDeserializer implements Deserializer<List<NormalizedTransaction>> {
    private ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public List<NormalizedTransaction> deserialize(String topic, byte[] data) {
        TypeReference<List<NormalizedTransaction>> typeRef
                = new TypeReference<List<NormalizedTransaction>>() {};
        List<NormalizedTransaction> snapshot = null;
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
