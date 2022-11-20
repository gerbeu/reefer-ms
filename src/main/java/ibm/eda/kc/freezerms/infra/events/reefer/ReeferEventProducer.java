package ibm.eda.kc.freezerms.infra.events.reefer;

import java.text.MessageFormat;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

import javax.enterprise.context.ApplicationScoped;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.context.Context;
import io.smallrye.reactive.messaging.TracingMetadata;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;

import static io.smallrye.reactive.messaging.kafka.KafkaConnector.TRACER;

@ApplicationScoped
public class ReeferEventProducer {
    Logger logger = Logger.getLogger(ReeferEventProducer.class.getName());
    @Channel("reefers")
    public Emitter<ReeferEvent> reeferEventProducer;


    public void sendEvent(String key, ReeferEvent reeferEvent) {
        logger.info("Send event -> " + reeferEvent.reeferID + " type of " + reeferEvent.getType() + " ts:" + reeferEvent.getTimestampMillis());
        Context context = Context.current();
        createReeferEventProducedSpan(reeferEvent, context);
        reeferEventProducer.send(Message.of(reeferEvent)
                .addMetadata(OutgoingKafkaRecordMetadata.<String>builder().withKey(key).build())
                .withAck(() -> {

                    return CompletableFuture.completedFuture(null);
                })
                .withNack(throwable -> {
                    return CompletableFuture.completedFuture(null);
                })
                .addMetadata(TracingMetadata.withCurrent(context))
        );
    }

    private void createReeferEventProducedSpan(final ReeferEvent reeferEvent, final Context context) {
        final String spanName = MessageFormat.format("produced event[{0}]", reeferEvent.getType());
        final SpanBuilder spanBuilder = TRACER.spanBuilder(spanName).setParent(context);
        final Span span = spanBuilder.startSpan();
        final ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        try {
            final String reeferEventJson = ow.writeValueAsString(reeferEvent);
            span.setAttribute("produced.reefer.event", reeferEventJson);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        } finally {
            span.end();
        }
    }

}
