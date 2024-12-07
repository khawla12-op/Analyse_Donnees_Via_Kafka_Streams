import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Materialized;

// Le but de cette application est de calculer le nombre total des ventes par client
public class TotalCommandeClient {
    public static void main(String[] args) {
        // Configuration des propriétés
        Properties props = new Properties();
        props.put("application.id", "order-processing-app");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("default.key.serde", Serdes.String().getClass());
        props.put("default.value.serde", Serdes.String().getClass());
        // Création du builder
        StreamsBuilder builder = new StreamsBuilder();

        // Lecture du topic "orders"
        KStream<String, String> ordersStream = builder.stream("orders");

        // Filtrage des commandes avec un total supérieur à 100
        KStream<String, String> filteredOrdersStream = ordersStream.filter((key, value) -> {
            System.out.println(key+":"+ value);
            double total = Double.parseDouble(value.split(",")[1]);
            return total > 100;
        });

        // Regroupement des commandes par client
        KGroupedStream<String, String> groupedStream = filteredOrdersStream.groupBy((key, value) ->
                value.split(",")[0]);

        // Agrégation pour calculer le total par client
        KTable<String, Double> totalByCustomer = groupedStream.aggregate(
                () -> 0.0,
                (key, value, sum) -> {
                    double total = Double.parseDouble(value.split(",")[1]);
                    return sum + total;
                },
                Materialized.with(Serdes.String(), Serdes.Double())
        );

        // Écriture des résultats dans le topic "customer-total"
        totalByCustomer.toStream().to("custumer-total");

        // Création et démarrage de l'application Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Ajout d'un hook pour arrêter proprement l'application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
