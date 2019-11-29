package com.github.microprograms.micro_refs;

import java.util.List;

import com.github.microprograms.micro_oss_core.MicroOssProvider;
import com.github.microprograms.micro_oss_core.QueryResult;
import com.github.microprograms.micro_oss_core.exception.MicroOssException;
import com.github.microprograms.micro_oss_core.model.Field;
import com.github.microprograms.micro_oss_core.model.ddl.CreateTableCommand;
import com.github.microprograms.micro_oss_core.model.ddl.DropTableCommand;
import com.github.microprograms.micro_oss_core.model.dml.query.Condition;
import com.github.microprograms.micro_oss_core.model.dml.query.PagerRequest;
import com.github.microprograms.micro_oss_core.model.dml.query.Sort;
import com.github.microprograms.micro_oss_core.utils.MicroOssUtils;
import com.github.microprograms.micro_refs.model.QueryRefResult;
import com.github.microprograms.micro_refs.model.Ref;
import com.github.microprograms.micro_refs.transaction.AbstractTransaction;
import com.github.microprograms.micro_refs.transaction.RefsTransactionFunctionalInterface;
import com.github.microprograms.micro_refs.utils.MicroRefsUtils;

public class MicroRefs implements Refs {

	private MicroOssProvider oss;

	public MicroRefs(MicroOssProvider oss) {
		this.oss = oss;
	}

	@Override
	public void createSchema(Class<?> clz, CreateTableCommand command) throws MicroOssException {
		command.getTableDefinition().setTableName(MicroOssUtils.getTableName(clz));
		oss.createTable(command);
	}

	@Override
	public void dropSchema(Class<?> clz) throws MicroOssException {
		oss.dropTable(new DropTableCommand(MicroOssUtils.getTableName(clz)));
	}

	@Override
	public int insert(Object object) throws MicroOssException {
		return oss.insertObject(object);
	}

	@Override
	public int update(Class<?> clz, List<Field> fields, Condition where) throws MicroOssException {
		return oss.updateObject(clz, fields, where);
	}

	@Override
	public int delete(Class<?> clz, Condition where) throws MicroOssException {
		return oss.deleteObject(clz, where);
	}

	@Override
	public int queryCount(Class<?> clz, Condition where) throws MicroOssException {
		return oss.queryCount(clz, where);
	}

	@Override
	public <T> QueryResult<T> query(Class<T> clz, Condition where) throws MicroOssException {
		return oss.query(clz, where);
	}

	@Override
	public <T> QueryResult<T> query(Class<T> clz, Condition where, List<Sort> sorts) throws MicroOssException {
		return oss.query(clz, where, sorts);
	}

	@Override
	public <T> QueryResult<T> query(Class<T> clz, Condition where, List<Sort> sorts, PagerRequest pager)
			throws MicroOssException {
		return oss.query(clz, where, sorts, pager);
	}

	@Override
	public <T> QueryResult<T> query(Class<T> clz, List<String> fieldNames, Condition where, List<Sort> sorts,
			PagerRequest pager) throws MicroOssException {
		return oss.query(clz, fieldNames, where, sorts, pager);
	}

	@Override
	public <S, T> void createRefSchema(Class<S> sourceClz, Class<T> targetClz) throws MicroOssException {
		oss.createTable(MicroRefsUtils.buildCreateTableCommand(sourceClz, targetClz));
	}

	@Override
	public <S, T> void dropRefSchema(Class<S> sourceClz, Class<T> targetClz) throws MicroOssException {
		oss.dropTable(MicroRefsUtils.buildDropTableCommand(sourceClz, targetClz));
	}

	@Override
	public int insertRef(Ref ref) throws MicroOssException {
		return oss.insertObject(MicroRefsUtils.buildInsertCommand(ref));
	}

	@Override
	public int updateRef(Ref ref) throws MicroOssException {
		return oss.updateObject(MicroRefsUtils.buildUpdateCommand(ref));
	}

	@Override
	public int deleteRef(Ref ref) throws MicroOssException {
		return oss.deleteObject(MicroRefsUtils.buildDeleteCommand(ref));
	}

	@Override
	public <S, T> int queryRefCount(Class<S> sourceClz, Class<T> targetClz, Condition sourceCondition,
			Condition targetCondition) throws MicroOssException {
		return oss.queryCount(MicroRefsUtils.buildSelectCountCommand_queryRefCount(sourceClz, targetClz,
				sourceCondition, targetCondition));
	}

	@Override
	public <S, T> QueryRefResult<S, T> queryRef(Class<S> sourceClz, Class<T> targetClz, Condition sourceCondition,
			Condition targetCondition) throws MicroOssException {
		return queryRef(sourceClz, targetClz, null, sourceCondition, targetCondition, null, null);
	}

	@Override
	public <S, T> QueryRefResult<S, T> queryRef(Class<S> sourceClz, Class<T> targetClz, Condition sourceCondition,
			Condition targetCondition, List<Sort> sorts) throws MicroOssException {
		return queryRef(sourceClz, targetClz, null, sourceCondition, targetCondition, sorts, null);
	}

	@Override
	public <S, T> QueryRefResult<S, T> queryRef(Class<S> sourceClz, Class<T> targetClz, Condition sourceCondition,
			Condition targetCondition, List<Sort> sorts, PagerRequest pager) throws MicroOssException {
		return queryRef(sourceClz, targetClz, null, sourceCondition, targetCondition, sorts, pager);
	}

	@Override
	public <S, T> QueryRefResult<S, T> queryRef(Class<S> sourceClz, Class<T> targetClz, List<String> fieldNames,
			Condition sourceCondition, Condition targetCondition, List<Sort> sorts, PagerRequest pager)
			throws MicroOssException {
		QueryResult<?> queryResult = oss.query(MicroRefsUtils.buildSelectCommand_queryRef(sourceClz, targetClz,
				fieldNames, sourceCondition, targetCondition, sorts, pager));
		return new QueryRefResult<>(sourceClz, targetClz, queryResult.getEntities());
	}

	@Override
	public <S, T> int queryNotRefCount(Class<S> sourceClz, Class<T> targetClz, Condition sourceCondition,
			Condition targetCondition) throws MicroOssException {
		return oss.queryCount(MicroRefsUtils.buildSelectCountCommand_queryNotRefCount(sourceClz, targetClz,
				sourceCondition, targetCondition));
	}

	@Override
	public <S, T> QueryRefResult<S, T> queryNotRef(Class<S> sourceClz, Class<T> targetClz, Condition sourceCondition,
			Condition targetCondition) throws MicroOssException {
		return queryNotRef(sourceClz, targetClz, null, sourceCondition, targetCondition, null, null);
	}

	@Override
	public <S, T> QueryRefResult<S, T> queryNotRef(Class<S> sourceClz, Class<T> targetClz, Condition sourceCondition,
			Condition targetCondition, List<Sort> sorts) throws MicroOssException {
		return queryNotRef(sourceClz, targetClz, null, sourceCondition, targetCondition, sorts, null);
	}

	@Override
	public <S, T> QueryRefResult<S, T> queryNotRef(Class<S> sourceClz, Class<T> targetClz, Condition sourceCondition,
			Condition targetCondition, List<Sort> sorts, PagerRequest pager) throws MicroOssException {
		return queryNotRef(sourceClz, targetClz, null, sourceCondition, targetCondition, sorts, pager);
	}

	@Override
	public <S, T> QueryRefResult<S, T> queryNotRef(Class<S> sourceClz, Class<T> targetClz, List<String> fieldNames,
			Condition sourceCondition, Condition targetCondition, List<Sort> sorts, PagerRequest pager)
			throws MicroOssException {
		QueryResult<?> queryResult = oss.query(MicroRefsUtils.buildSelectCommand_queryNotRef(sourceClz, targetClz,
				fieldNames, sourceCondition, targetCondition, sorts, pager));
		return new QueryRefResult<>(sourceClz, targetClz, queryResult.getEntities());
	}

	@Override
	public void execute(RefsTransactionFunctionalInterface transaction) throws MicroOssException {
		oss.execute(new AbstractTransaction() {
			@Override
			public void execute(MicroOssProvider microOssProvider) throws MicroOssException {
				transaction.execute(new MicroRefs(microOssProvider));
			}
		});
	}
}
