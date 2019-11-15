package com.github.microprograms.micro_refs.transaction;

import java.util.UUID;

import com.github.microprograms.micro_oss_core.Transaction;

public abstract class AbstractTransaction implements Transaction {

	private String transactionId = UUID.randomUUID().toString();

	@Override
	public String getTransactionId() {
		return transactionId;
	}

}
