import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

//Cette classe permet de convertir en majuscule:
public class ConvertirMajuscule {
    public static void main(String[] args){
        // Configuration des propriétés
        Properties props = new Properties();
        props.put("application.id", "kafka-streams-app");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("default.key.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");
        props.put("default.value.serde","org.apache.kafka.common.serialization.Serdes$StringSerde");
        // Création du builder
        StreamsBuilder builder = new StreamsBuilder();

        // Exemple de logique de streaming simple
        KStream<String, String> sourceStream = builder.stream("input-topic");
        //convertir en majuscules
        KStream<String, String> sinkStream = sourceStream.mapValues(value ->
        {
            System.out.println(value);
            return  value.toUpperCase();
        });
        sinkStream.to("output-topic");
        // Création et démarrage de l'application Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}

