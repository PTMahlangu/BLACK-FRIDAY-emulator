package com.example.waiting_room.store;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Seeds the Black Friday catalogue. Prices are in South African rand cents (R1 = 100).
 * Existing products keep their stock but get the current price.
 */
@Component
@RequiredArgsConstructor
public class ProductSeeder implements ApplicationRunner {

    private static final List<Product> CATALOGUE = List.of(
            new Product("65\" OLED TV (doorbuster)", 24_999_00, 50),
            new Product("Games console", 9_999_00, 150),
            new Product("Noise-cancelling headphones", 4_999_00, 300),
            new Product("Smartwatch", 2_999_00, 400),
            new Product("Air fryer", 1_299_00, 800),
            new Product("USB-C charger", 299_00, 2_000)
    );

    private final ProductRepository products;

    @Override
    @Transactional
    public void run(ApplicationArguments args) {
        Map<String, Product> existing = products.findAll().stream()
                .collect(Collectors.toMap(Product::getName, Function.identity(), (a, b) -> a));
        for (Product seed : CATALOGUE) {
            Product current = existing.get(seed.getName());
            if (current == null) {
                products.save(new Product(seed.getName(), seed.getPriceCents(), seed.getInitialStock()));
            } else {
                current.setPriceCents(seed.getPriceCents());
            }
        }
    }
}
