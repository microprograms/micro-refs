package com.github.microprograms.micro_refs.model;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class Ref {
	private List<Location> locations;
	private long createAt;
	private String properties;

	public Ref() {
	}

	public Ref(List<Location> locations) {
		this(locations, System.currentTimeMillis(), "{}");
	}

	public Ref(List<Location> locations, String properties) {
		this(locations, System.currentTimeMillis(), properties);
	}

	public Ref(List<Location> locations, long createAt, String properties) {
		this.locations = locations;
		this.createAt = createAt;
		this.properties = properties;

		Collections.sort(this.locations, new Comparator<Location>() {
			@Override
			public int compare(Location o1, Location o2) {
				return o1.getClz().getSimpleName().compareTo(o2.getClz().getSimpleName());
			}
		});
	}

	public String getRefTableName() {
		return String.format("_ref_%s_%s", locations.get(0).getClz().getSimpleName(),
				locations.get(1).getClz().getSimpleName());
	}

	public static <S, T> Ref build(Class<S> sourceClz, String sourceId, Class<T> targetClz, String targetId) {
		Ref.Location sourceRefLocation = new Ref.Location(sourceClz, sourceId);
		Ref.Location targetRefLocation = new Ref.Location(targetClz, targetId);
		return new Ref(Arrays.asList(sourceRefLocation, targetRefLocation));
	}

	public static <S, T> Ref build(Class<S> sourceClz, String sourceId, Class<T> targetClz, String targetId,
			String properties) {
		Ref.Location sourceRefLocation = new Ref.Location(sourceClz, sourceId);
		Ref.Location targetRefLocation = new Ref.Location(targetClz, targetId);
		return new Ref(Arrays.asList(sourceRefLocation, targetRefLocation), properties);
	}

	public static <S, T> Ref build(Class<S> sourceClz, String sourceId, Class<T> targetClz, String targetId,
			long createAt, String properties) {
		Ref.Location sourceRefLocation = new Ref.Location(sourceClz, sourceId);
		Ref.Location targetRefLocation = new Ref.Location(targetClz, targetId);
		return new Ref(Arrays.asList(sourceRefLocation, targetRefLocation), createAt, properties);
	}

	public static <S, T> Ref build(Ref.Location sourceRefLocation, Ref.Location targetRefLocation) {
		return new Ref(Arrays.asList(sourceRefLocation, targetRefLocation));
	}

	public static <S, T> Ref build(Ref.Location sourceRefLocation, Ref.Location targetRefLocation, String properties) {
		return new Ref(Arrays.asList(sourceRefLocation, targetRefLocation), properties);
	}

	public static <S, T> Ref build(Ref.Location sourceRefLocation, Ref.Location targetRefLocation, long createAt,
			String properties) {
		return new Ref(Arrays.asList(sourceRefLocation, targetRefLocation), createAt, properties);
	}

	public List<Location> getLocations() {
		return locations;
	}

	public void setLocations(List<Location> locations) {
		this.locations = locations;
	}

	public long getCreateAt() {
		return createAt;
	}

	public void setCreateAt(long createAt) {
		this.createAt = createAt;
	}

	public String getProperties() {
		return properties;
	}

	public void setProperties(String properties) {
		this.properties = properties;
	}

	public static class Location {
		private Class<?> clz;
		private String id;

		public Location() {
		}

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
