package com.example.waiting_room.store;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Getter
@Setter
@NoArgsConstructor
public class Product {

    @Id
    @GeneratedValue
    private Long id;

    private String name;

    private long priceCents;

    private int stock;

    private int initialStock;

    public Product(String name, long priceCents, int stock) {
        this.name = name;
        this.priceCents = priceCents;
        this.stock = stock;
        this.initialStock = stock;
    }
}
