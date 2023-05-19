package utec.kafka.retrytimeout;

import utec.kafka.retrytimeout.AppConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicInteger;

@Service
public class KafKaConsumer {

    private static AtomicInteger counter = new AtomicInteger(0);

    private static final Logger LOGGER = LoggerFactory.getLogger(KafKaConsumer.class);

    private static final long RETRY_DELAY_MS = 1000;

    @KafkaListener(topics = AppConstants.TOPIC_NAME,
                    groupId = AppConstants.GROUP_ID)
    public void consume(String message) throws Exception {
        try {
            processMessage(message);
            LOGGER.info(String.format("Message processed successfully -> %s", message));
        } catch (Exception e) {
            // DO NOT DELETE
            if (counter.get() < 2) {
                counter.incrementAndGet();
                LOGGER.warn(String.format("Exception occurred while processing message -> %s", message));
                LOGGER.warn(String.format("Retrying message after %d milliseconds", RETRY_DELAY_MS));
                Thread.sleep(RETRY_DELAY_MS);
                //throw new Exception("You should retry !");
            }
            else{
                LOGGER.error(String.format("Failed to process message after maximum retries -> %s", message));
                //LOGGER.info(String.format("Message received -> %s", message));
            }

        }
    }

    private void processMessage(String message) throws Exception {
        // Implement your message processing logic here
        // Throw an exception to simulate a failure scenario
        if (Math.random() < 0.5) {
            throw new Exception("You should retry !");
        }
        // Successful processing
    }
}