package fp;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;



public class CSVProducer {

	private static String KafkaEndpoint = "localhost:9092";
    private static String KafkaTopic = "twitter-kafka";
    private static String CsvFile = "weatherHistory.csv";
    
  
    private Producer<String, String> ProducerProperties(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaEndpoint);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaCsvProducer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<String, String>(properties);
    }

    public static void main(String[] args) throws URISyntaxException {
        
    	CSVProducer kafkaProducer = new CSVProducer();
        kafkaProducer.PublishMessages();
    }

    
    
    private void PublishMessages() throws URISyntaxException{
    	
        final Producer<String, String> csvProducer = ProducerProperties();
        
        try{
        	URI uri = getClass().getClassLoader().getResource(CsvFile).toURI();
            Stream<String> FileStream = Files.lines(Paths.get(uri));
            
            FileStream.forEach(line -> {
            	
                final ProducerRecord<String, String> csvRecord = new ProducerRecord<String, String>(
                        KafkaTopic, UUID.randomUUID().toString(), line.toString());

                csvProducer.send(csvRecord, (metadata, exception) -> {
                    if(metadata != null){
                        System.out.println("CsvData: -> "+ csvRecord.key()+" | "+ csvRecord.value());
                    }
                    else{
                        System.out.println("Error Sending Csv Record -> "+ csvRecord.value());
                    }
                });
            });

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}