package com.example.orderledger;

import org.springframework.boot.SpringApplication;

public class TestOrderLedgerApplication {

	public static void main(String[] args) {
		SpringApplication.from(OrderLedgerApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
