package traindb.jdbc.core;

import java.sql.SQLException;
import java.sql.Types;

import traindb.jdbc.util.ByteConverter;
import traindb.jdbc.util.GT;
import traindb.jdbc.util.TrainDBJdbcException;
import traindb.jdbc.util.TrainDBState;

public class ParameterList {
	private final int paramCount;
	private final Object[] paramValues;
	private final int[] paramTypes;
	private final byte[] flags;
	
	private static final Object NULL_OBJECT = new Object();

	private static final byte IN = 1;
	private static final byte OUT = 2;
	private static final byte INOUT = IN | OUT;

	private static final byte TEXT = 0;
	private static final byte BINARY = 4;

	public ParameterList(String sql) {
		this.paramCount = getParamCount(sql);
		this.paramValues = new Object[this.paramCount];
		this.paramTypes = new int[this.paramCount];
		this.flags = new byte[paramCount];
	}

	private int getParamCount(String sql) {
		return (int) sql.chars().filter(c -> c == '?').count();
	}
	
	public int getParamCount() {
		return this.paramCount;
	}
	
	private void bind(int index, Object value, int type, byte binary) throws SQLException {
		if (index < 1 || index > paramValues.length) {
			throw new TrainDBJdbcException(GT.tr("The column index is out of range: {0}, number of columns: {1}.", index, paramValues.length), TrainDBState.INVALID_PARAMETER_VALUE);
		}

	    --index;

	    paramValues[index] = value;
	    flags[index] = (byte) (direction(index) | IN | binary);

	    if (type == Types.NULL && paramTypes[index] != Types.NULL && value == NULL_OBJECT) {
	    	return;
	    }

	    paramTypes[index] = type;
	}
	
	public void setNull(int index, int type) throws SQLException {
		bind(index, NULL_OBJECT, type, TEXT);
	}

	public void setStringParameter(int paramIndex, String s, int type) throws SQLException {
		bind(paramIndex, s, type, TEXT);
	}
	
	public void setIntParameter(int paramIndex, int value) throws SQLException {
		byte[] data = new byte[4];
		ByteConverter.int4(data, 0, value);
		bind(paramIndex, data, Types.INTEGER, BINARY);
	}
	
	public void setLiteralParameter(int paramIndex, String s, int type) throws SQLException {
		bind(paramIndex, s, type, TEXT);
	}
	
	private byte direction(int index) {
		return (byte) (flags[index] & INOUT);
	}
	
	public byte[] getFlags() {
		return flags;
	}
	
	public String toString(int index, boolean standardConformingStrings) {
		--index;
		Object paramValue = paramValues[index];
		if (paramValue == null) {
			return "?";
		} else if (paramValue == NULL_OBJECT) {
			return "NULL";
		} else if ((flags[index] & BINARY) == BINARY) {
			// handle some of the numeric types

			switch (paramTypes[index]) {
			case Types.SMALLINT:
				short s = ByteConverter.int2((byte[]) paramValue, 0);
				return Short.toString(s);

			case Types.INTEGER:
				int i = ByteConverter.int4((byte[]) paramValue, 0);
				return Integer.toString(i);

			case Types.BIGINT:
				long l = ByteConverter.int8((byte[]) paramValue, 0);
				return Long.toString(l);

			case Types.FLOAT:
				float f = ByteConverter.float4((byte[]) paramValue, 0);
				if (Float.isNaN(f)) {
					return "'NaN'::real";
				}
				return Float.toString(f);

			case Types.DOUBLE:
				double d = ByteConverter.float8((byte[]) paramValue, 0);
				if (Double.isNaN(d)) {
					return "'NaN'::double precision";
				}
				return Double.toString(d);

			case Types.NUMERIC:
				Number n = ByteConverter.numeric((byte[]) paramValue);
				if (n instanceof Double) {
					assert ((Double) n).isNaN();
					return "'NaN'::numeric";
				}
				return n.toString();

			/*
			case Oid.UUID:
				String uuid = new UUIDArrayAssistant().buildElement((byte[]) paramValue, 0, 16).toString();
				return "'" + uuid + "'::uuid";

			case Oid.POINT:
				PGpoint pgPoint = new PGpoint();
				pgPoint.setByteValue((byte[]) paramValue, 0);
				return "'" + pgPoint.toString() + "'::point";

			case Oid.BOX:
				PGbox pgBox = new PGbox();
				pgBox.setByteValue((byte[]) paramValue, 0);
				return "'" + pgBox.toString() + "'::box";
			*/
			}
				
			return "?";
		} else {
			String param = paramValue.toString();

			// add room for quotes + potential escaping.
			StringBuilder p = new StringBuilder(3 + (param.length() + 10) / 10 * 11);

			// No E'..' here since escapeLiteral escapes all things and it does not use \123
			// kind of
			// escape codes
			p.append('\'');
			try {
				p = Utils.escapeLiteral(p, param, standardConformingStrings);
			} catch (SQLException sqle) {
				// This should only happen if we have an embedded null
				// and there's not much we can do if we do hit one.
				//
				// The goal of toString isn't to be sent to the server,
				// so we aren't 100% accurate (see StreamWrapper), put
				// the unescaped version of the data.
				//
				p.append(param);
			}
			p.append('\'');
			int paramType = paramTypes[index];
			if (paramType == Types.TIMESTAMP) {
				p.append("::timestamp");
			} else if (paramType == Types.TIMESTAMP_WITH_TIMEZONE) {
				p.append("::timestamp with time zone");
			} else if (paramType == Types.TIME) {
				p.append("::time");
			} else if (paramType == Types.TIME_WITH_TIMEZONE) {
				p.append("::time with time zone");
			} else if (paramType == Types.DATE) {
				p.append("::date");
		        /*
			} else if (paramType == Oid.INTERVAL) {
				p.append("::interval");
		        */
			} else if (paramType == Types.NUMERIC) {
				p.append("::numeric");
			}
			return p.toString();
		}
	}
}
