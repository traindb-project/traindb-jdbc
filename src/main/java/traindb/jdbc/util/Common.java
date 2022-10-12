package traindb.jdbc.util;

import java.util.Enumeration;
import java.util.Properties;

public class Common {
	public static Properties mergeProperties(Properties proFrom, Properties proTo) throws TrainDBException {
		if (proFrom != null && proTo != null) {
			Enumeration<?> e = proFrom.propertyNames();
			while (e.hasMoreElements()) {
				String propName = (String) e.nextElement();
				String propValue = proFrom.getProperty(propName);
				if (propValue == null) {
					throw new TrainDBException(GT.tr("Properties for the driver contains a non-string value for the key ") + propName, TrainDBState.UNEXPECTED_ERROR);
				}
				proTo.setProperty(propName, propValue);
			}
		}
		
		return proTo;
	}
}
