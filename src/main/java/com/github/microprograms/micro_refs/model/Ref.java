package com.github.microprograms.micro_refs.model;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class Ref {
	private List<Location> locations;
	private String label = "";
	private String comment = "";
	private String properties = "{}";
	private long createAt = System.currentTimeMillis();
	private long updateAt = 0;

	public Ref(List<Location> locations) {
		setLocations(locations);
	}

	public String getRefTableName() {
		return String.format("_ref_%s_%s", locations.get(0).getClz().getSimpleName(),
				locations.get(1).getClz().getSimpleName());
	}

	public List<Location> getLocations() {
		return locations;
	}

	public void setLocations(List<Location> locations) {
		this.locations = locations;

		Collections.sort(this.locations, new Comparator<Location>() {
			@Override
			public int compare(Location o1, Location o2) {
				return o1.getClz().getSimpleName().compareTo(o2.getClz().getSimpleName());
			}
		});
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public String getProperties() {
		return properties;
	}

	public void setProperties(String properties) {
		this.properties = properties;
	}

	public long getCreateAt() {
		return createAt;
	}

	public void setCreateAt(long createAt) {
		this.createAt = createAt;
	}

	public long getUpdateAt() {
		return updateAt;
	}

	public void setUpdateAt(long updateAt) {
		this.updateAt = updateAt;
	}

	public static class Location {
		private Class<?> clz;
		private String id;

		public Location(Class<?> clz) {
			this.clz = clz;
		}

		public Location(Class<?> clz, String id) {
			this.clz = clz;
			this.id = id;
		}

		public String getRefIdFieldName() {
			return String.format("ref_id_%s", clz.getSimpleName());
		}

		public Class<?> getClz() {
			return clz;
		}

		public void setClz(Class<?> clz) {
			this.clz = clz;
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}
	}
}
