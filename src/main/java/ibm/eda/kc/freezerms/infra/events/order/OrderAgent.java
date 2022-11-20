package ibm.eda.kc.freezerms.infra.events.order;

import java.text.MessageFormat;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.smallrye.reactive.messaging.TracingMetadata;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import ibm.eda.kc.freezerms.domain.Reefer;
import ibm.eda.kc.freezerms.infra.events.reefer.ReeferAllocated;
import ibm.eda.kc.freezerms.infra.events.reefer.ReeferEvent;
import ibm.eda.kc.freezerms.infra.events.reefer.ReeferEventProducer;
import ibm.eda.kc.freezerms.infra.repo.ReeferRepository;

import static io.smallrye.reactive.messaging.kafka.KafkaConnector.TRACER;

/**
 * Listen to the orders topic and processes event from order service:
 * - order created event
 * - order cancelled event
 * Normally it should also support order updated event and recompute capacity
 */
@ApplicationScoped
public class OrderAgent {
    Logger logger = Logger.getLogger(OrderAgent.class.getName());

    @Inject
    ReeferRepository repo;

    @Inject
    ReeferEventProducer reeferEventProducer;

    @Incoming("orders")
    public CompletionStage<Void> processOrder(Message<OrderEvent> messageWithOrderEvent) {
        logger.info("Received order : " + messageWithOrderEvent.getPayload().orderID);
        OrderEvent orderEvent = messageWithOrderEvent.getPayload();
        Optional<TracingMetadata> optionalTracingMetadata = TracingMetadata.fromMessage(messageWithOrderEvent);
        if (optionalTracingMetadata.isPresent()) {
            TracingMetadata tracingMetadata = optionalTracingMetadata.get();
            Context context = tracingMetadata.getCurrentContext();
            try (Scope scope = context.makeCurrent()) {
                createProcessedOrderEventSpan(orderEvent, context);
                switch (orderEvent.getType()) {
                    case OrderEvent.ORDER_CREATED_TYPE:
                        processOrderCreatedEvent(orderEvent);
                        break;
                    case OrderEvent.ORDER_UPDATED_TYPE:
                        logger.info("Receive order update " + orderEvent.status);
                        compensateOrder(orderEvent.orderID);
                        break;
                    default:
                        break;
                }
            }
        }
        return messageWithOrderEvent.ack();
    }

    private void createProcessedOrderEventSpan(final OrderEvent orderEvent, final Context context) {
        final String spanName = MessageFormat.format("processed event[{0}]", orderEvent.getType());
        final SpanBuilder spanBuilder = TRACER.spanBuilder(spanName).setParent(context);
        final Span span = spanBuilder.startSpan();
        final ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        try {
            final String orderEventJson = ow.writeValueAsString(orderEvent);
            span.setAttribute("processed.order.event", orderEventJson);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        } finally {
            span.end();
        }
    }

    /**
     * When order created, search for reefers close to the pickup location,
     * add them in the container ids and send an event as ReeferAllocated
     */
    public ReeferEvent processOrderCreatedEvent(OrderEvent oe) {
        OrderCreatedEvent oce = (OrderCreatedEvent) oe.payload;
        List<Reefer> reefers = repo.getReefersForOrder(oe.orderID,
                oce.pickupCity,
                oe.quantity);
        if (!reefers.isEmpty()) {
            ReeferAllocated reeferAllocatedEvent = new ReeferAllocated(reefers, oe.orderID);
            ReeferEvent re = new ReeferEvent(ReeferEvent.REEFER_ALLOCATED_TYPE, reeferAllocatedEvent);
            re.reeferID = reeferAllocatedEvent.reeferIDs;
            reeferEventProducer.sendEvent(re.reeferID, re);
            return re;
        }
        return null;
    }

    public void compensateOrder(String oid) {
        repo.cleanTransaction(oid);
    }
}
