package com.example.waiting_room.store;

import com.example.waiting_room.events.EventPublisher;
import com.example.waiting_room.events.EventType;
import com.example.waiting_room.events.ShopEvent;
import com.example.waiting_room.waitingroom.WaitingRoomService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.LongAdder;

@Service
@RequiredArgsConstructor
public class StoreService {

    public enum PurchaseResult { PURCHASED, SOLD_OUT, NOT_ADMITTED, UNKNOWN_PRODUCT }

    private final ProductRepository products;
    private final PurchaseOrderRepository orders;
    private final WaitingRoomService waitingRoom;
    private final EventPublisher events;

    private final LongAdder purchased = new LongAdder();
    private final LongAdder soldOutRejections = new LongAdder();
    private final LongAdder revenueCents = new LongAdder();

    /** Only shoppers currently admitted to the store can buy. Stock can never go below zero. */
    @Transactional
    public PurchaseResult purchase(UUID ticketId, Long productId) {
        if (!waitingRoom.isShopping(ticketId)) {
            return PurchaseResult.NOT_ADMITTED;
        }
        Product product = products.findById(productId).orElse(null);
        if (product == null) {
            return PurchaseResult.UNKNOWN_PRODUCT;
        }
        if (products.takeOne(productId) == 0) {
            soldOutRejections.increment();
            events.publish(ShopEvent.of(EventType.SOLD_OUT, ticketId, productId));
            return PurchaseResult.SOLD_OUT;
        }
        orders.save(new PurchaseOrder(productId, ticketId, product.getPriceCents()));
        purchased.increment();
        revenueCents.add(product.getPriceCents());
        events.publish(ShopEvent.of(EventType.PURCHASED, ticketId, productId));
        return PurchaseResult.PURCHASED;
    }

    public List<Product> catalogue() {
        return products.findAll().stream()
                .sorted((a, b) -> Long.compare(a.getId(), b.getId()))
                .toList();
    }

    @Transactional
    public void reset() {
        orders.deleteAllInBatch();
        products.restockAll();
        purchased.reset();
        soldOutRejections.reset();
        revenueCents.reset();
    }

    public Stats stats() {
        return new Stats(purchased.sum(), soldOutRejections.sum(), revenueCents.sum());
    }

    public record Stats(long purchased, long soldOutRejections, long revenueCents) {
    }
}
