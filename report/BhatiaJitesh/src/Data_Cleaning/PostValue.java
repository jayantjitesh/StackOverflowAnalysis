package com.mr.project;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class PostValue implements Writable {
	private IntWritable type;
	private Text tag;
	private LongWritable id;
	private LongWritable parentId;
	private LongWritable ownerUserId;
	private Text creationDate;
	private Text closedDate;
	private LongWritable viewCount;
	private LongWritable answerCount;

	public IntWritable getType() {
		return type;
	}

	public LongWritable getOwnerUserId() {
		return ownerUserId;
	}

	public void setOwnerUserId(LongWritable ownerUserId) {
		this.ownerUserId = ownerUserId;
	}

	public void setType(IntWritable type) {
		this.type = type;
	}

	public Text getTag() {
		return tag;
	}

	public void setTag(Text tag) {
		this.tag = tag;
	}

	public LongWritable getId() {
		return id;
	}

	public void setId(LongWritable id) {
		this.id = id;
	}

	public LongWritable getParentId() {
		return parentId;
	}

	public void setParentId(LongWritable parentId) {
		this.parentId = parentId;
	}

	public Text getCreationDate() {
		return creationDate;
	}

	public void setCreationDate(Text creationDate) {
		this.creationDate = creationDate;
	}

	public Text getClosedDate() {
		return closedDate;
	}

	public void setClosedDate(Text closedDate) {
		this.closedDate = closedDate;
	}

	public LongWritable getViewCount() {
		return viewCount;
	}

	public void setViewCount(LongWritable viewCount) {
		this.viewCount = viewCount;
	}

	public LongWritable getAnswerCount() {
		return answerCount;
	}

	public void setAnswerCount(LongWritable answerCount) {
		this.answerCount = answerCount;
	}

	public IntWritable getFlightMonth() {
		return type;
	}

	public void setFlightMonth(IntWritable flightMonth) {
		this.type = flightMonth;
	}

	public Text getFlightNum() {
		return tag;
	}

	public void setFlightNum(Text flightNum) {
		this.tag = flightNum;
	}

	/**
	 * Default constructor to be used by Map reduce for serializing the data
	 */
	public PostValue() {
		this.tag = new Text();
		this.type = new IntWritable();
		this.id = new LongWritable();
		this.parentId = new LongWritable();
		this.ownerUserId = new LongWritable();
		this.answerCount = new LongWritable();
		this.viewCount = new LongWritable();
		this.closedDate = new Text();
		this.creationDate = new Text();

	}

	public PostValue(PostValue pv) {
		this.tag = new Text(pv.tag.toString());
		this.type = new IntWritable(pv.type.get());
		this.id = new LongWritable(pv.id.get());
		this.parentId = new LongWritable(pv.parentId.get());
		this.ownerUserId =new LongWritable(pv.ownerUserId.get()); 
		this.answerCount = new LongWritable(pv.answerCount.get());
		this.viewCount = new LongWritable(pv.viewCount.get());
		this.closedDate = new Text(pv.closedDate.toString());
		this.creationDate = new Text(pv.creationDate.toString());

	}

	@Override
	public void write(DataOutput out) throws IOException {
		id.write(out);
		parentId.write(out);
		ownerUserId.write(out);
		tag.write(out);
		type.write(out);
		answerCount.write(out);
		viewCount.write(out);
		creationDate.write(out);
		closedDate.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id.readFields(in);
		parentId.readFields(in);
		ownerUserId.readFields(in);
		tag.readFields(in);
		type.readFields(in);
		answerCount.readFields(in);
		viewCount.readFields(in);
		creationDate.readFields(in);
		closedDate.readFields(in);

	}

	public static PostValue createPost(String[] values) {
		PostValue value = new PostValue();
		value.setId(new LongWritable(Long.parseLong(values[0])));
		value.setType(new IntWritable(Integer.parseInt(values[1])));
		value.setParentId(new LongWritable(Long.parseLong(values[2])));
		value.setCreationDate(new Text(values[3]));
		value.setViewCount(new LongWritable(Long.parseLong(values[4])));
		value.setOwnerUserId(new LongWritable(Long.parseLong(values[5])));
		value.setClosedDate(new Text(values[6]));
		value.setTag(new Text(values[7]));
		value.setAnswerCount(new LongWritable(Long.parseLong(values[8])));
		return value;

	}

	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append(type);
		buffer.append(",");
		buffer.append(parentId);
		buffer.append(",");
		buffer.append(creationDate);
		buffer.append(",");
		buffer.append(viewCount);
		buffer.append(",");
		buffer.append(ownerUserId);
		buffer.append(",");
		buffer.append(closedDate);
		buffer.append(",");
		buffer.append(tag);
		buffer.append(",");
		buffer.append(answerCount);
		return buffer.toString();
	}

}
