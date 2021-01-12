package spark.sessionizer;

import java.util.ArrayList;
import java.util.Arrays;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Serializable;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.immutable.List;
import scala.reflect.ClassTag;

public class UserSession implements Serializable {

	// required?
	public UserSession() {

	}

	private UserSession(String userId/*, List<AnalyticsEvent> events,*/, boolean active) {
		this.userId = userId;
		//this.events = events;
		this.active = active;
	}

	public static UserSession createNew(Iterator<Row> rows) {
		java.util.List<AnalyticsEvent> analyticsEvents = new ArrayList<>();
		while(rows.hasNext()) {
			analyticsEvents.add(AnalyticsEvent.of(rows.next()));
		}

		return new UserSession(analyticsEvents.get(0).userId(),/*, List.empty(),*/ true);
	}

	public void expire() {
		active = false;
	}

	@Override
	public String toString() {
		return "UserSession{" +
			// "_events=" + events +
			", _userId='" + userId + '\'' +
			", _active=" + active +
			'}';
	}

	public String getUserId() {
		return userId;
	}

	public boolean isActive() {
		return active;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public void setActive(boolean active) {
		this.active = active;
	}

//	public List<AnalyticsEvent> getEvents() {
//		return events;
//	}
//
//	public void setEvents(List events) {
//		this.events = events;
//	}

	//private List<AnalyticsEvent> events;
	private String userId;
	private boolean active = true;

	public UserSession updateEvents(Iterator<Row> rows) {
//		java.util.List<AnalyticsEvent> analyticsEvents = new ArrayList<>();
//
//		while(rows.hasNext()) {
//			analyticsEvents.add(AnalyticsEvent.of(rows.next()));
//		}
//
//		java.util.List<AnalyticsEvent> newAnalyticsEvents =
//			new ArrayList<AnalyticsEvent>(){{
//				addAll(analyticsEvents);
//			}};
//
//		java.util.Iterator<AnalyticsEvent> analyticsEventIterator =
//			JavaConverters.asJavaIterator(events);
//
//		analyticsEventIterator.forEachRemaining(newAnalyticsEvents::add);
//
//		newAnalyticsEvents.addAll(analyticsEvents);

		return new UserSession(userId,/*, List.empty(),*/ active);
	}
}
