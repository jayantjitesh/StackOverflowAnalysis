package com.mr.project;

public class Record  implements Comparable<Record>{
	private String userName;
	private Long count;

	public Record(Long count2, String userName2) {
		this.count=count2;
		this.userName=userName2;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public Long getCount() {
		return count;
	}

	public void setCount(Long count) {
		this.count = count;
	}

	@Override
	public int compareTo(Record o) {
		return count.compareTo(o.getCount());
	}

}
