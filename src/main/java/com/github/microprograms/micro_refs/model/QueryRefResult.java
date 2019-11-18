package com.github.microprograms.micro_refs.model;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSONObject;
import com.github.microprograms.micro_oss_core.QueryResult;
import com.github.microprograms.micro_oss_core.model.Entity;

public class QueryRefResult<S, T> extends QueryResult<T> {

	private Class<S> sourceClz;
	private Class<T> targetClz;

	public QueryRefResult(Class<S> sourceClz, Class<T> targetClz, List<Entity> entities) {
		super(entities, targetClz);
		this.sourceClz = sourceClz;
		this.targetClz = targetClz;
	}

	private Ref _buildRef(Entity entity) {
		RefBuilder refBuilder = new RefBuilder();
		JSONObject json = entity.toJson();
		Ref.Location sourceRefLocation = new Ref.Location(sourceClz);
		sourceRefLocation.setId(json.getString(sourceRefLocation.getRefIdFieldName()));
		refBuilder.location(sourceRefLocation);
		Ref.Location targetRefLocation = new Ref.Location(targetClz);
		targetRefLocation.setId(json.getString(targetRefLocation.getRefIdFieldName()));
		refBuilder.location(targetRefLocation);
		refBuilder.label(json.getString("ref_label"));
		refBuilder.comment(json.getString("ref_comment"));
		refBuilder.properties(json.getString("ref_properties"));
		refBuilder.order(json.getInteger("ref_order"));
		refBuilder.createAt(json.getLongValue("ref_createAt"));
		return refBuilder.build();
	}

	public Ref getRef() {
		if (getEntities().isEmpty()) {
			return null;
		}
		return _buildRef(getEntities().get(0));
	}

	public List<Ref> getAllRefs() {
		List<Ref> list = new ArrayList<>();
		for (Entity x : getEntities()) {
			list.add(_buildRef(x));
		}
		return list;
	}

	public Class<S> getSourceClz() {
		return sourceClz;
	}

	public Class<T> getTargetClz() {
		return targetClz;
	}
}
