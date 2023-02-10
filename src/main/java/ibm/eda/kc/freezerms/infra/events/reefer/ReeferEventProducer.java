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
import io.opentelemetry.context.Scope;
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
        sendEventWithContext(key, reeferEvent, context);
    }

    private void sendEventWithContext(String key, ReeferEvent reeferEvent, Context context) {
        final Span span = startSpan(reeferEvent, context);
        try {
            final String reeferEventJson = serializeReeferEvent(reeferEvent);
            addEventAttributeToSpan(span, reeferEventJson);
            sendReeferEventWithSpanContext(key, reeferEvent, span);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        } finally {
            endSpan(span);
        }
    }

    private Span startSpan(ReeferEvent reeferEvent, Context context) {
        final String spanName = formatSpanName(reeferEvent);
        final SpanBuilder spanBuilder = TRACER.spanBuilder(spanName).setParent(context);
        return spanBuilder.startSpan();
    }

    private String formatSpanName(ReeferEvent reeferEvent) {
        return MessageFormat.format("produced event[{0}]", reeferEvent.getType());
    }

    private String serializeReeferEvent(ReeferEvent reeferEvent) throws JsonProcessingException {
        final ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        return ow.writeValueAsString(reeferEvent);
    }

    private void addEventAttributeToSpan(Span span, String reeferEventJson) {
        span.setAttribute("produced.event", reeferEventJson);
    }

    private void sendReeferEventWithSpanContext(String key, ReeferEvent reeferEvent, Span span) {
        final Context spanContext = Context.current().with(span);
        try (Scope scope = spanContext.makeCurrent()) {
            reeferEventProducer.send(Message.of(reeferEvent)
                    .addMetadata(OutgoingKafkaRecordMetadata.<String>builder().withKey(key).build())
                    .withAck(() -> CompletableFuture.completedFuture(null))
                    .withNack(throwable -> CompletableFuture.completedFuture(null))
                    .addMetadata(TracingMetadata.withCurrent(spanContext))
            );
        }
    }

    private void endSpan(Span span) {
        span.end();
    }

}
