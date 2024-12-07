import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.common.serialization.Serdes;

// Le but de cette application est de collecter des données météorologiques en temps réel via Kafka.
public class DonneesMeteorologiques {
    public static void main(String[] args) {
        // Configuration des propriétés
        Properties props = new Properties();
        props.put("application.id", "weather-data-app");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("default.key.serde", Serdes.String().getClass());
        props.put("default.value.serde", Serdes.String().getClass());
        // Création du builder
        StreamsBuilder builder = new StreamsBuilder();

        // Lecture du topic "weather-data"
        KStream<String, String> weatherStream = builder.stream("weather-data");

        // Filtrage des donnees ou la temperature est supérieure a 30:
        KStream<String, String> filteredWeatherStream = weatherStream.filter((key, value) -> {
            try {
                String[] parts = value.split(",");
                if (parts.length < 3) return false;
                double temperature = Double.parseDouble(parts[1]);
                boolean passesFilter = temperature > 30;
                System.out.println("Filtered: " + value + " -> " + passesFilter);
                return passesFilter;
            } catch (Exception e) {
                System.err.println("Error processing record: " + value + " - " + e.getMessage());
                return false; // Ignorer les enregistrements non conformes
            }
        });


        //Conversion de la temperature en Fahrenheit
        KStream<String,String> convertedStream= filteredWeatherStream.mapValues(value->{
            String[] parts =value.split(",");
            String station = parts[0];
            double celsius = Double.parseDouble(parts[1]);
            double fahrenheit = (celsius * 9 /5)+32;
            String humidity= parts[2];
            return station+","+fahrenheit+","+humidity;
        });

        // Regroupement par station:
        KGroupedStream<String, String> groupedStream = convertedStream.groupBy(
                (key, value) -> value.split(",")[0],
                Grouped.with(Serdes.String(),Serdes.String())
        );
        // Calcul des moyennes de température et d'humidité
        KTable<String, String> aggregatedTable = groupedStream.aggregate(
                () -> "0.0,0.0,0",
                (station, newValue, aggregate) -> {
                    try {
                        String[] newValueParts = newValue.split(",");
                        String[] aggregateParts = aggregate.split(",");

                        if (newValueParts.length < 3 || aggregateParts.length < 3) {
                            throw new IllegalArgumentException("Invalid data format");
                        }

                        double newTemp = Double.parseDouble(newValueParts[1]);
                        double newHumidity = Double.parseDouble(newValueParts[2]);
                        double totalTemp = Double.parseDouble(aggregateParts[0]) + newTemp;
                        double totalHumidity = Double.parseDouble(aggregateParts[1]) + newHumidity;
                        int count = Integer.parseInt(aggregateParts[2]) + 1;

                        return totalTemp + "," + totalHumidity + "," + count;
                    } catch (Exception e) {
                        System.err.println("Error aggregating record: " + newValue + " - " + e.getMessage());
                        return aggregate; // Retourner l'agrégat inchangé en cas d'erreur
                    }
                },
                Materialized.with(Serdes.String(), Serdes.String())
        );

        // Calcul des moyennes et conversion pour le topic de sortie
        KStream<String, String> resultStream = aggregatedTable.toStream().mapValues(aggregate -> {
            String[] parts = aggregate.split(",");
            double totalTemp = Double.parseDouble(parts[0]);
            double totalHumidity = Double.parseDouble(parts[1]);
            int count = Integer.parseInt(parts[2]);

            double avgTemp = totalTemp / count;
            double avgHumidity = totalHumidity / count;

            return "Température Moyenne = " + avgTemp + "F, Humidité Moyenne = " + avgHumidity + "%";
        });
        // Écriture des résultats dans le topic "station-averages"
        resultStream.to("station-averages",Produced.with(Serdes.String(), Serdes.String()));;

        // Création et démarrage de l'application Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Ajout d'un hook pour arrêter proprement l'application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
