package com.example.orderledger.api;

import com.example.orderledger.store.InventoryState;
import com.example.orderledger.store.LedgerRepository;
import com.example.orderledger.store.OrderLineState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LedgerControllerTest {

    @Mock
    private LedgerRepository repository;

    private LedgerController controller;

    @BeforeEach
    void setUp() {
        controller = new LedgerController(repository);
    }

    @Test
    void returnsInventory() {
        var inventory = new InventoryState("SKU-1", -2, 5, true, 4);
        when(repository.findInventory("SKU-1")).thenReturn(Optional.of(inventory));

        assertThat(controller.inventory("SKU-1")).isEqualTo(inventory);
    }

    @Test
    void returnsAnOrderProjection() {
        var lines = List.of(
                new OrderLineState("order-1", "SKU-A", 2, "PLACED", 1),
                new OrderLineState("order-1", "SKU-B", 1, "PLACED", 1)
        );
        when(repository.findOrderLines("order-1")).thenReturn(lines);

        assertThat(controller.order("order-1"))
                .isEqualTo(new OrderView("order-1", "PLACED", 1, lines));
    }

    @Test
    void returnsNotFoundForAnUnknownOrder() {
        when(repository.findOrderLines("missing")).thenReturn(List.of());

        assertThatThrownBy(() -> controller.order("missing"))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        exception -> assertThat(exception.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND));
    }
}

