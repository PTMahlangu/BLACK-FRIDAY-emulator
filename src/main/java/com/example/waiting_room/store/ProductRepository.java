package com.example.waiting_room.store;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface ProductRepository extends JpaRepository<Product, Long> {

    /** Atomically takes one unit of stock. Returns 1 on success, 0 if sold out. */
    @Modifying
    @Query("update Product p set p.stock = p.stock - 1 where p.id = :id and p.stock > 0")
    int takeOne(@Param("id") Long id);

    @Modifying
    @Query("update Product p set p.stock = p.initialStock")
    void restockAll();
}
