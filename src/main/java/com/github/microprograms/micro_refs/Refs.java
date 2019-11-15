package com.github.microprograms.micro_refs;

import java.util.List;

import com.github.microprograms.micro_oss_core.QueryResult;
import com.github.microprograms.micro_oss_core.exception.MicroOssException;
import com.github.microprograms.micro_oss_core.model.Field;
import com.github.microprograms.micro_oss_core.model.ddl.CreateTableCommand;
import com.github.microprograms.micro_oss_core.model.dml.query.Condition;
import com.github.microprograms.micro_oss_core.model.dml.query.PagerRequest;
import com.github.microprograms.micro_oss_core.model.dml.query.Sort;
import com.github.microprograms.micro_refs.model.QueryRefResult;
import com.github.microprograms.micro_refs.model.Ref;
import com.github.microprograms.micro_refs.transaction.RefsTransactionFunctionalInterface;

public interface Refs {

	void createSchema(Class<?> clz, CreateTableCommand command) throws MicroOssException;

	void dropSchema(Class<?> clz) throws MicroOssException;

	int insert(Object object) throws MicroOssException;

	int update(Class<?> clz, List<Field> fields, Condition where) throws MicroOssException;

	int delete(Class<?> clz, Condition where) throws MicroOssException;

	int queryCount(Class<?> clz, Condition where) throws MicroOssException;

	<T> QueryResult<T> query(Class<T> clz, Condition where) throws MicroOssException;

	<T> QueryResult<T> query(Class<T> clz, Condition where, List<Sort> sorts) throws MicroOssException;

	<T> QueryResult<T> query(Class<T> clz, Condition where, List<Sort> sorts, PagerRequest pager)
			throws MicroOssException;

	<T> QueryResult<T> query(Class<T> clz, List<String> fieldNames, Condition where, List<Sort> sorts,
			PagerRequest pager) throws MicroOssException;

	<S, T> void createRefSchema(Class<S> sourceClz, Class<T> targetClz) throws MicroOssException;

	<S, T> void dropRefSchema(Class<S> sourceClz, Class<T> targetClz) throws MicroOssException;

	int insertRef(Ref ref) throws MicroOssException;

	int updateRef(Ref ref) throws MicroOssException;

	int deleteRef(Ref ref) throws MicroOssException;

	<S, T> int queryRefCount(Class<S> sourceClz, Class<T> targetClz, Condition sourceCondition,
			Condition targetCondition) throws MicroOssException;

	<S, T> QueryRefResult<S, T> queryRef(Class<S> sourceClz, Class<T> targetClz, Condition sourceCondition,
			Condition targetCondition) throws MicroOssException;

	<S, T> QueryRefResult<S, T> queryRef(Class<S> sourceClz, Class<T> targetClz, Condition sourceCondition,
			Condition targetCondition, List<Sort> sorts) throws MicroOssException;

	<S, T> QueryRefResult<S, T> queryRef(Class<S> sourceClz, Class<T> targetClz, Condition sourceCondition,
			Condition targetCondition, List<Sort> sorts, PagerRequest pager) throws MicroOssException;

	<S, T> QueryRefResult<S, T> queryRef(Class<S> sourceClz, Class<T> targetClz, List<String> fieldNames,
			Condition sourceCondition, Condition targetCondition, List<Sort> sorts, PagerRequest pager)
			throws MicroOssException;

	void execute(RefsTransactionFunctionalInterface transaction) throws MicroOssException;
}
