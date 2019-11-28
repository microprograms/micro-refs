package com.github.microprograms.micro_refs.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
import com.github.microprograms.micro_refs.model.Ref;

import org.apache.commons.lang3.StringUtils;

public class MicroRefsUtils {

    public static String getTableName(Class<?> clz, String tablePrefix) {
        if (StringUtils.isBlank(tablePrefix)) {
            return clz.getSimpleName();
        }
        return tablePrefix + clz.getSimpleName();
    }

    public static String getRefTableName(Ref ref, String tablePrefix) {
        if (StringUtils.isBlank(tablePrefix)) {
            return ref.getRefTableName();
        }
        return tablePrefix + ref.getRefTableName();
    }

    public static <S, T> CreateTableCommand buildCreateTableCommand(Class<S> sourceClz, Class<T> targetClz,
            String tablePrefix) {
        Ref.Location sourceRefLocation = new Ref.Location(sourceClz);
        Ref.Location targetRefLocation = new Ref.Location(targetClz);
        Ref ref = new Ref(Arrays.asList(sourceRefLocation, targetRefLocation));
        String tableName = getRefTableName(ref, tablePrefix);
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
        return new CreateTableCommand(new TableDefinition(tableName, tableComment, fields));
    }

    public static <S, T> DropTableCommand buildDropTableCommand(Class<S> sourceClz, Class<T> targetClz,
            String tablePrefix) {
        Ref.Location sourceRefLocation = new Ref.Location(sourceClz);
        Ref.Location targetRefLocation = new Ref.Location(targetClz);
        Ref ref = new Ref(Arrays.asList(sourceRefLocation, targetRefLocation));
        String tableName = getRefTableName(ref, tablePrefix);
        return new DropTableCommand(tableName);
    }

    public static InsertCommand buildInsertCommand(Ref ref, String tablePrefix) {
        String tableName = getRefTableName(ref, tablePrefix);
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
        return new InsertCommand(new Entity(tableName, fields));
    }

    public static UpdateCommand buildUpdateCommand(Ref ref, String tablePrefix) {
        String tableName = getRefTableName(ref, tablePrefix);
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
        return new UpdateCommand(tableName, fields, Condition.and(conditions));
    }

    public static DeleteCommand buildDeleteCommand(Ref ref, String tablePrefix) {
        String tableName = getRefTableName(ref, tablePrefix);
        Condition[] conditions = new Condition[ref.getLocations().size()];
        for (int i = 0; i < ref.getLocations().size(); i++) {
            Ref.Location location = ref.getLocations().get(i);
            String fieldName = location.getRefIdFieldName();
            conditions[i] = Condition.build(fieldName + "=", location.getId());
        }
        return new DeleteCommand(tableName, Condition.and(conditions));
    }

    public static <S, T> SelectCountCommand buildSelectCountCommand_queryRefCount(Class<S> sourceClz,
            Class<T> targetClz, Condition sourceCondition, Condition targetCondition, String tablePrefix) {
        Ref.Location sourceRefLocation = new Ref.Location(sourceClz);
        Ref.Location targetRefLocation = new Ref.Location(targetClz);
        String refTableName = getRefTableName(new Ref(Arrays.asList(sourceRefLocation, targetRefLocation)),
                tablePrefix);
        String sourceTableName = getTableName(sourceClz, tablePrefix);
        String targetTableName = getTableName(targetClz, tablePrefix);
        Condition sourceJoinCondition = Condition.and(sourceCondition,
                Condition.raw(String.format("ref.%s=", sourceRefLocation.getRefIdFieldName()), "source.id"));
        Condition targetJoinCondition = Condition.and(targetCondition,
                Condition.raw(String.format("ref.%s=", targetRefLocation.getRefIdFieldName()), "target.id"));
        List<Join> joins = new ArrayList<>();
        joins.add(new Join(Join.TypeEnum.join, String.format("%s source", sourceTableName), sourceJoinCondition));
        joins.add(new Join(Join.TypeEnum.join, String.format("%s target", targetTableName), targetJoinCondition));
        return new SelectCountCommand(String.format("%s ref", refTableName), joins, null);
    }

    public static <S, T> SelectCommand buildSelectCommand_queryRef(Class<S> sourceClz, Class<T> targetClz,
            List<String> fieldNames, Condition sourceCondition, Condition targetCondition, List<Sort> sorts,
            PagerRequest pager, String tablePrefix) throws MicroOssException {
        Ref.Location sourceRefLocation = new Ref.Location(sourceClz);
        Ref.Location targetRefLocation = new Ref.Location(targetClz);
        String refTableName = getRefTableName(new Ref(Arrays.asList(sourceRefLocation, targetRefLocation)),
                tablePrefix);
        String sourceTableName = getTableName(sourceClz, tablePrefix);
        String targetTableName = getTableName(targetClz, tablePrefix);
        if (null == fieldNames) {
            fieldNames = new ArrayList<>();
        }
        if (fieldNames.isEmpty()) {
            fieldNames.add("ref.*");
            fieldNames.add("target.*");
        }
        if (null == sorts) {
            sorts = new ArrayList<>();
        }
        if (sorts.isEmpty()) {
            sorts.add(Sort.asc("ref.ref_order"));
            sorts.add(Sort.desc("ref.ref_createAt"));
        }
        Condition sourceJoinCondition = Condition.and(sourceCondition,
                Condition.raw(String.format("ref.%s=", sourceRefLocation.getRefIdFieldName()), "source.id"));
        Condition targetJoinCondition = Condition.and(targetCondition,
                Condition.raw(String.format("ref.%s=", targetRefLocation.getRefIdFieldName()), "target.id"));
        List<Join> joins = new ArrayList<>();
        joins.add(new Join(Join.TypeEnum.join, String.format("%s source", sourceTableName), sourceJoinCondition));
        joins.add(new Join(Join.TypeEnum.join, String.format("%s target", targetTableName), targetJoinCondition));
        return new SelectCommand(String.format("%s ref", refTableName), fieldNames, joins, null, sorts, pager);
    }

    public static <S, T> SelectCountCommand buildSelectCountCommand_queryNotRefCount(Class<S> sourceClz,
            Class<T> targetClz, Condition sourceCondition, Condition targetCondition, String tablePrefix)
            throws MicroOssException {
        Ref.Location sourceRefLocation = new Ref.Location(sourceClz);
        Ref.Location targetRefLocation = new Ref.Location(targetClz);
        String refTableName = getRefTableName(new Ref(Arrays.asList(sourceRefLocation, targetRefLocation)),
                tablePrefix);
        String sourceTableName = getTableName(sourceClz, tablePrefix);
        String targetTableName = getTableName(targetClz, tablePrefix);
        Condition sourceJoinCondition = Condition.and(sourceCondition,
                Condition.raw(String.format("ref.%s=", sourceRefLocation.getRefIdFieldName()), "source.id"));
        Condition targetJoinCondition = Condition.and(targetCondition,
                Condition.raw(String.format("ref.%s=", targetRefLocation.getRefIdFieldName()), "target.id"));
        List<Join> joins = new ArrayList<>();
        joins.add(new Join(Join.TypeEnum.join, String.format("%s source", sourceTableName), sourceJoinCondition));
        joins.add(new Join(Join.TypeEnum.rightJoin, String.format("%s target", targetTableName), targetJoinCondition));
        return new SelectCountCommand(String.format("%s ref", refTableName), joins,
                Condition.build("source.id is", null));
    }

    public static <S, T> SelectCommand buildSelectCommand_queryNotRef(Class<S> sourceClz, Class<T> targetClz,
            List<String> fieldNames, Condition sourceCondition, Condition targetCondition, List<Sort> sorts,
            PagerRequest pager, String tablePrefix) throws MicroOssException {
        Ref.Location sourceRefLocation = new Ref.Location(sourceClz);
        Ref.Location targetRefLocation = new Ref.Location(targetClz);
        String refTableName = getRefTableName(new Ref(Arrays.asList(sourceRefLocation, targetRefLocation)),
                tablePrefix);
        String sourceTableName = getTableName(sourceClz, tablePrefix);
        String targetTableName = getTableName(targetClz, tablePrefix);
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
        return new SelectCommand(String.format("%s ref", refTableName), fieldNames, joins,
                Condition.build("source.id is", null), sorts, pager);
    }
}
