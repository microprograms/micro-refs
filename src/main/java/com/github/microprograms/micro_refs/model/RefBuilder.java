package com.github.microprograms.micro_refs.model;

import java.util.ArrayList;
import java.util.List;

import com.github.microprograms.micro_refs.model.Ref.Location;

public class RefBuilder {
	private List<Location> locations = new ArrayList<>();
	private String label = "";
	private String comment = "";
	private String properties = "{}";
	private long createAt = System.currentTimeMillis();

	public RefBuilder location(Class<?> clz, String id) {
		locations.add(new Location(clz, id));
		return this;
	}

	public RefBuilder location(Location location) {
		locations.add(location);
		return this;
	}

	public RefBuilder locations(List<Location> locations) {
		this.locations.addAll(locations);
		return this;
	}

	public RefBuilder label(String label) {
		this.label = label;
		return this;
	}

	public RefBuilder comment(String comment) {
		this.comment = comment;
		return this;
	}

	public RefBuilder properties(String properties) {
		this.properties = properties;
		return this;
	}

	public RefBuilder createAt(long createAt) {
		this.createAt = createAt;
		return this;
	}

	public Ref build() {
		Ref ref = new Ref(locations);
		ref.setLabel(label);
		ref.setComment(comment);
		ref.setProperties(properties);
		ref.setCreateAt(createAt);
		return ref;
	}
}
