package spark.sessionizer;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.GroupState;
import scala.Function1;
import scala.Function3;
import scala.Option;
import scala.Serializable;
import scala.Tuple3;
import scala.collection.Iterator;
import scala.collection.JavaConverters;

public class Mapping {

	public static class EventsToSessions
		implements Function3<String, Iterator<Row>, GroupState<UserSession>, String>, Serializable {

		@Override
		public String apply(String userId, scala.collection.Iterator<Row> rows, GroupState<UserSession> currentState) {
			if (currentState.hasTimedOut()) {
				UserSession userSession = currentState.get();

				userSession.expire();

				return userSession.getUserId() + " expired";
			}

			Option<UserSession> userSessionOption = currentState.getOption();

			UserSession userSession = userSessionOption
				.map(activeUserSession -> activeUserSession.updateEvents(rows))
				.getOrElse(() -> UserSession.createNew(rows));

			currentState.update(userSession);
			//currentState.setTimeoutTimestamp(currentState.getCurrentProcessingTimeMs() + TimeUnit.MINUTES.toMillis(2));
			currentState.setTimeoutDuration("1 minute");

			return null;
		}
	}

	public static class EventsToEvents
		implements Function3<String, Iterator<Row>, GroupState<UserSession>, Iterator<String>>, Serializable {


		@Override
		public Iterator<String> apply(String userId, Iterator<Row> rows, GroupState<UserSession> currentState) {
			if (currentState.hasTimedOut()) {
				UserSession userSession = currentState.get();

				userSession.expire();

				List<String> list = Arrays.asList("{" + userId + ", expired = true }");

				return JavaConverters.asScalaIterator(list.iterator());
			}

			Option<UserSession> userSessionOption = currentState.getOption();

			UserSession userSession = userSessionOption
				.map(activeUserSession -> activeUserSession.updateEvents(rows))
				.getOrElse(() -> UserSession.createNew(rows));

			currentState.update(userSession);
			currentState.setTimeoutTimestamp(currentState.getCurrentProcessingTimeMs() + TimeUnit.MINUTES.toMillis(2));

			List<String> list = Arrays.asList("{" + userId + ", expired = false }");

			return JavaConverters.asScalaIterator(list.iterator());
		}


	}
}
