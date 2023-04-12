package traindb.jdbc.util;

import java.sql.SQLException;

@SuppressWarnings("serial")
public class TrainDBJdbcException extends SQLException {
	private ServerErrorMessage _serverError;

    public TrainDBJdbcException(String msg, TrainDBState state, Throwable cause)
    {
        super(msg, state == null ? null : state.getState());
        initCause(cause);
    }

    public TrainDBJdbcException(String msg)
    {
    	this(msg, null, null);
    }
    
    public TrainDBJdbcException(String msg, TrainDBState state)
    {
        this(msg, state, null);
        // super(msg, state == null ? null : state.getState());
    }

    public TrainDBJdbcException(ServerErrorMessage serverError)
    {
        this(serverError.toString(), new TrainDBState(serverError.getSQLState()));
        _serverError = serverError;
    }
    
    public ServerErrorMessage getServerErrorMessage()
    {
        return _serverError;
    }
}
