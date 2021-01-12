package spark.sessionizer;

import java.io.Serializable;
import org.apache.spark.sql.Row;

public class AnalyticsEvent implements Serializable {
	public static AnalyticsEvent of(Row row) {
		String userId = row.getAs("userId");
		String eventId = row.getAs("eventId");

		return new AnalyticsEvent(userId, eventId);
	}

	public String userId() {
		return _userId;
	}

	public String eventId() {
		return _eventId;
	}

	private AnalyticsEvent(String userId, String eventId) {
		_userId = userId;
		_eventId = eventId;
	}

	@Override
	public String toString() {
		return "AnalyticsEvent{" +
			"_userId='" + _userId + '\'' +
			", _eventId='" + _eventId + '\'' +
			'}';
	}

	private String _userId;
	private String _eventId;
}
