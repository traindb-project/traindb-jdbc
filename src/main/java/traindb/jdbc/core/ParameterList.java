package traindb.jdbc.core;

import java.sql.SQLException;

import traindb.jdbc.util.ByteConverter;
import traindb.jdbc.util.GT;
import traindb.jdbc.util.TrainDBException;
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
	
	private void bind(int index, Object value, int oid, byte binary) throws SQLException {
		if (index < 1 || index > paramValues.length) {
			throw new TrainDBException(GT.tr("The column index is out of range: {0}, number of columns: {1}.", index, paramValues.length), TrainDBState.INVALID_PARAMETER_VALUE);
		}

	    --index;

	    paramValues[index] = value;
	    flags[index] = (byte) (direction(index) | IN | binary);

	    if (oid == Oid.UNSPECIFIED && paramTypes[index] != Oid.UNSPECIFIED && value == NULL_OBJECT) {
	    	return;
	    }

	    paramTypes[index] = oid;
	}
	
	public void setNull(int index, int oid) throws SQLException {
		bind(index, NULL_OBJECT, oid, TEXT);
	}

	public void setStringParameter(int paramIndex, String s, int oid) throws SQLException {
		bind(paramIndex, s, oid, TEXT);
	}
	
	public void setIntParameter(int paramIndex, int value) throws SQLException {
		byte[] data = new byte[4];
		ByteConverter.int4(data, 0, value);
		bind(paramIndex, data, Oid.INT4, BINARY);
	}
	
	public void setLiteralParameter(int paramIndex, String s, int oid) throws SQLException {
		bind(paramIndex, s, oid, TEXT);
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
			case Oid.INT2:
				short s = ByteConverter.int2((byte[]) paramValue, 0);
				return Short.toString(s);

			case Oid.INT4:
				int i = ByteConverter.int4((byte[]) paramValue, 0);
				return Integer.toString(i);

			case Oid.INT8:
				long l = ByteConverter.int8((byte[]) paramValue, 0);
				return Long.toString(l);

			case Oid.FLOAT4:
				float f = ByteConverter.float4((byte[]) paramValue, 0);
				if (Float.isNaN(f)) {
					return "'NaN'::real";
				}
				return Float.toString(f);

			case Oid.FLOAT8:
				double d = ByteConverter.float8((byte[]) paramValue, 0);
				if (Double.isNaN(d)) {
					return "'NaN'::double precision";
				}
				return Double.toString(d);

			case Oid.NUMERIC:
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
			if (paramType == Oid.TIMESTAMP) {
				p.append("::timestamp");
			} else if (paramType == Oid.TIMESTAMPTZ) {
				p.append("::timestamp with time zone");
			} else if (paramType == Oid.TIME) {
				p.append("::time");
			} else if (paramType == Oid.TIMETZ) {
				p.append("::time with time zone");
			} else if (paramType == Oid.DATE) {
				p.append("::date");
			} else if (paramType == Oid.INTERVAL) {
				p.append("::interval");
			} else if (paramType == Oid.NUMERIC) {
				p.append("::numeric");
			}
			return p.toString();
		}
	}
}
