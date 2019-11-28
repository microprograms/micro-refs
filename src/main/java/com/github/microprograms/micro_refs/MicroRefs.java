package com.github.microprograms.micro_refs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.github.microprograms.micro_oss_core.MicroOssProvider;
import com.github.microprograms.micro_oss_core.QueryResult;
import com.github.microprograms.micro_oss_core.exception.MicroOssException;
import com.github.microprograms.micro_oss_core.model.Entity;
import com.github.microprograms.micro_oss_core.model.Field;
import com.github.microprograms.micro_oss_core.model.FieldDefinition;
import com.github.microprograms.micro_oss_core.model.FieldDefinition.FieldTypeEnum;
import com.github.microprograms.micro_oss_core.model.TableDefinition;
import com.github.microprograms.micro_oss_core.model.ddl.CreateTableCommand;
import com.github.microprograms.micro_oss_core.model.ddl.DropTableCommand;
import com.github.microprograms.micro_oss_core.model.dml.query.Condition;
import com.github.microprograms.micro_oss_core.model.dml.query.Join;
import com.github.microprograms.micro_oss_core.model.dml.query.PagerRequest;
import com.github.microprograms.micro_oss_core.model.dml.query.SelectCommand;
import com.github.microprograms.micro_oss_core.model.dml.query.SelectCountCommand;
import com.github.microprograms.micro_oss_core.model.dml.query.Sort;
import com.github.microprograms.micro_oss_core.model.dml.update.DeleteCommand;
import com.github.microprograms.micro_oss_core.model.dml.update.InsertCommand;
import com.github.microprograms.micro_oss_core.model.dml.update.UpdateCommand;
import com.github.microprograms.micro_refs.model.QueryRefResult;
import com.github.microprograms.micro_refs.model.Ref;
import com.github.microprograms.micro_refs.transaction.AbstractTransaction;
import com.github.microprograms.micro_refs.transaction.RefsTransactionFunctionalInterface;

import org.apache.commons.lang3.StringUtils;

public class MicroRefs implements Refs {

	private MicroOssProvider oss;

	public MicroRefs(MicroOssProvider oss) {
		this.oss = oss;
	}

	private String _getTableName(Class<?> clz) {
		String prefix = oss.getConfig().getTablePrefix();
		if (StringUtils.isBlank(prefix)) {
			return clz.getSimpleName();
		}
		return prefix + clz.getSimpleName();
	}

	@Override
	public void createSchema(Class<?> clz, CreateTableCommand command) throws MicroOssException {
		command.getTableDefinition().setTableName(_getTableName(clz));
		oss.createTable(command);
	}

	@Override
	public void dropSchema(Class<?> clz) throws MicroOssException {
		oss.dropTable(new DropTableCommand(_getTableName(clz)));
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

	private String _getRefTableName(Ref ref) {
		String prefix = oss.getConfig().getTablePrefix();
		if (StringUtils.isBlank(prefix)) {
			return ref.getRefTableName();
		}
		return prefix + ref.getRefTableName();
	}

	@Override
	public <S, T> void createRefSchema(Class<S> sourceClz, Class<T> targetClz) throws MicroOssException {
		Ref.Location sourceRefLocation = new Ref.Location(sourceClz);
		Ref.Location targetRefLocation = new Ref.Location(targetClz);
		Ref ref = new Ref(Arrays.asList(sourceRefLocation, targetRefLocation));
		String tableName = _getRefTableName(ref);
		String tableComment = "ref";
		List<FieldDefinition> fields = new ArrayList<FieldDefinition>();
		for (int i = 0; i < ref.getLocations().size(); i++) {
			Ref.Location location = ref.getLocations().get(i);
			String clzName = location.getClz().getSimpleName();
			String fieldName = location.getRefIdFieldName();
			String fieldComment = String.format("%s ID", clzName);
			FieldTypeEnum fieldType = FieldTypeEnum.text_type;
			int primaryKey = i + 1;
			fields.add(new FieldDefinition(fieldName, fieldComment, fieldType, "", primaryKey));
		}
		fields.add(new FieldDefinition("ref_label", "标签", FieldTypeEnum.text_type, "", 0));
		fields.add(new FieldDefinition("ref_comment", "备注", FieldTypeEnum.text_type, "", 0));
		fields.add(new FieldDefinition("ref_properties", "属性", FieldTypeEnum.text_type, "", 0));
		fields.add(new FieldDefinition("ref_order", "排序号", FieldTypeEnum.long_type, "", 0));
		fields.add(new FieldDefinition("ref_createAt", "创建时间", FieldTypeEnum.long_type, "", 0));
		fields.add(new FieldDefinition("ref_updateAt", "更新时间", FieldTypeEnum.long_type, "", 0));
		oss.createTable(new CreateTableCommand(new TableDefinition(tableName, tableComment, fields)));
	}

	@Override
	public <S, T> void dropRefSchema(Class<S> sourceClz, Class<T> targetClz) throws MicroOssException {
		Ref.Location sourceRefLocation = new Ref.Location(sourceClz);
		Ref.Location targetRefLocation = new Ref.Location(targetClz);
		Ref ref = new Ref(Arrays.asList(sourceRefLocation, targetRefLocation));
		String tableName = _getRefTableName(ref);
		oss.dropTable(new DropTableCommand(tableName));
	}

	@Override
	public int insertRef(Ref ref) throws MicroOssException {
		String tableName = _getRefTableName(ref);
		List<Field> fields = new ArrayList<>();
		for (int i = 0; i < ref.getLocations().size(); i++) {
			Ref.Location location = ref.getLocations().get(i);
			String fieldName = location.getRefIdFieldName();
			fields.add(new Field(fieldName, location.getId()));
		}
		fields.add(new Field("ref_label", ref.getLabel()));
		fields.add(new Field("ref_comment", ref.getComment()));
		fields.add(new Field("ref_properties", ref.getProperties()));
		fields.add(new Field("ref_order", ref.getOrder()));
		fields.add(new Field("ref_createAt", ref.getCreateAt()));
		return oss.insertObject(new InsertCommand(new Entity(tableName, fields)));
	}

	@Override
	public int updateRef(Ref ref) throws MicroOssException {
		String tableName = _getRefTableName(ref);
		Condition[] conditions = new Condition[ref.getLocations().size()];
		for (int i = 0; i < ref.getLocations().size(); i++) {
			Ref.Location location = ref.getLocations().get(i);
			String fieldName = location.getRefIdFieldName();
			conditions[i] = Condition.build(fieldName + "=", location.getId());
		}
		List<Field> fields = new ArrayList<>();
		fields.add(new Field("ref_label", ref.getLabel()));
		fields.add(new Field("ref_comment", ref.getComment()));
		fields.add(new Field("ref_properties", ref.getProperties()));
		fields.add(new Field("ref_order", ref.getOrder()));
		fields.add(new Field("ref_updateAt", System.currentTimeMillis()));
		return oss.updateObject(new UpdateCommand(tableName, fields, Condition.and(conditions)));
	}

	@Override
	public int deleteRef(Ref ref) throws MicroOssException {
		String tableName = _getRefTableName(ref);
		Condition[] conditions = new Condition[ref.getLocations().size()];
		for (int i = 0; i < ref.getLocations().size(); i++) {
			Ref.Location location = ref.getLocations().get(i);
			String fieldName = location.getRefIdFieldName();
			conditions[i] = Condition.build(fieldName + "=", location.getId());
		}
		return oss.deleteObject(new DeleteCommand(tableName, Condition.and(conditions)));
	}

	@Override
	public <S, T> int queryRefCount(Class<S> sourceClz, Class<T> targetClz, Condition sourceCondition,
			Condition targetCondition) throws MicroOssException {
		Ref.Location sourceRefLocation = new Ref.Location(sourceClz);
		Ref.Location targetRefLocation = new Ref.Location(targetClz);
		String refTableName = _getRefTableName(new Ref(Arrays.asList(sourceRefLocation, targetRefLocation)));
		String sourceTableName = _getTableName(sourceClz);
		String targetTableName = _getTableName(targetClz);
		Condition sourceJoinCondition = Condition.and(sourceCondition,
				Condition.raw(String.format("ref.%s=", sourceRefLocation.getRefIdFieldName()), "source.id"));
		Condition targetJoinCondition = Condition.and(targetCondition,
				Condition.raw(String.format("ref.%s=", targetRefLocation.getRefIdFieldName()), "target.id"));
		List<Join> joins = new ArrayList<>();
		joins.add(new Join(Join.TypeEnum.join, String.format("%s source", sourceTableName), sourceJoinCondition));
		joins.add(new Join(Join.TypeEnum.join, String.format("%s target", targetTableName), targetJoinCondition));
		return oss.queryCount(new SelectCountCommand(String.format("%s ref", refTableName), joins, null));
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
		Ref.Location sourceRefLocation = new Ref.Location(sourceClz);
		Ref.Location targetRefLocation = new Ref.Location(targetClz);
		String refTableName = _getRefTableName(new Ref(Arrays.asList(sourceRefLocation, targetRefLocation)));
		String sourceTableName = _getTableName(sourceClz);
		String targetTableName = _getTableName(targetClz);
		if (null == fieldNames) {
			fieldNames = new ArrayList<>();
		}
		if (fieldNames.isEmpty()) {
			fieldNames.add("ref.*");
			fieldNames.add("target.*");
		}
		Condition sourceJoinCondition = Condition.and(sourceCondition,
				Condition.raw(String.format("ref.%s=", sourceRefLocation.getRefIdFieldName()), "source.id"));
		Condition targetJoinCondition = Condition.and(targetCondition,
				Condition.raw(String.format("ref.%s=", targetRefLocation.getRefIdFieldName()), "target.id"));
		List<Join> joins = new ArrayList<>();
		joins.add(new Join(Join.TypeEnum.join, String.format("%s source", sourceTableName), sourceJoinCondition));
		joins.add(new Join(Join.TypeEnum.join, String.format("%s target", targetTableName), targetJoinCondition));
		QueryResult<?> queryResult = oss
				.query(new SelectCommand(String.format("%s ref", refTableName), fieldNames, joins, null, sorts, pager));
		return new QueryRefResult<>(sourceClz, targetClz, queryResult.getEntities());
	}

	@Override
	public <S, T> int queryNotRefCount(Class<S> sourceClz, Class<T> targetClz, Condition sourceCondition,
			Condition targetCondition) throws MicroOssException {
		Ref.Location sourceRefLocation = new Ref.Location(sourceClz);
		Ref.Location targetRefLocation = new Ref.Location(targetClz);
		String refTableName = _getRefTableName(new Ref(Arrays.asList(sourceRefLocation, targetRefLocation)));
		String sourceTableName = _getTableName(sourceClz);
		String targetTableName = _getTableName(targetClz);
		Condition sourceJoinCondition = Condition.and(sourceCondition,
				Condition.raw(String.format("ref.%s=", sourceRefLocation.getRefIdFieldName()), "source.id"));
		Condition targetJoinCondition = Condition.and(targetCondition,
				Condition.raw(String.format("ref.%s=", targetRefLocation.getRefIdFieldName()), "target.id"));
		List<Join> joins = new ArrayList<>();
		joins.add(new Join(Join.TypeEnum.join, String.format("%s source", sourceTableName), sourceJoinCondition));
		joins.add(new Join(Join.TypeEnum.rightJoin, String.format("%s target", targetTableName), targetJoinCondition));
		return oss.queryCount(new SelectCountCommand(String.format("%s ref", refTableName), joins,
				Condition.build("source.id is", null)));
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
		Ref.Location sourceRefLocation = new Ref.Location(sourceClz);
		Ref.Location targetRefLocation = new Ref.Location(targetClz);
		String refTableName = _getRefTableName(new Ref(Arrays.asList(sourceRefLocation, targetRefLocation)));
		String sourceTableName = _getTableName(sourceClz);
		String targetTableName = _getTableName(targetClz);
		if (null == fieldNames) {
			fieldNames = new ArrayList<>();
		}
		if (fieldNames.isEmpty()) {
			fieldNames.add("ref.*");
			fieldNames.add("target.*");
		}
		Condition sourceJoinCondition = Condition.and(sourceCondition,
				Condition.raw(String.format("ref.%s=", sourceRefLocation.getRefIdFieldName()), "source.id"));
		Condition targetJoinCondition = Condition.and(targetCondition,
				Condition.raw(String.format("ref.%s=", targetRefLocation.getRefIdFieldName()), "target.id"));
		List<Join> joins = new ArrayList<>();
		joins.add(new Join(Join.TypeEnum.join, String.format("%s source", sourceTableName), sourceJoinCondition));
		joins.add(new Join(Join.TypeEnum.rightJoin, String.format("%s target", targetTableName), targetJoinCondition));
		QueryResult<?> queryResult = oss.query(new SelectCommand(String.format("%s ref", refTableName), fieldNames,
				joins, Condition.build("source.id is", null), sorts, pager));
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
