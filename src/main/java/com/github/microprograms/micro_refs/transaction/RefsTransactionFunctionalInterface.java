package com.github.microprograms.micro_refs.transaction;

import com.github.microprograms.micro_oss_core.exception.MicroOssException;
import com.github.microprograms.micro_refs.Refs;

@FunctionalInterface
public interface RefsTransactionFunctionalInterface {
	void execute(Refs refs) throws MicroOssException;
}
