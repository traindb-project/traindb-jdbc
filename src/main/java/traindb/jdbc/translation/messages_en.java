package traindb.jdbc.translation;

import java.util.Enumeration;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

public class messages_en extends ResourceBundle {
	private static final String[] table;
	static {
		String[] t = new String[74];
		t[0] = "";
	    t[1] = "Project-Id-Version: JDBC PostgreSQL Driver\nReport-Msgid-Bugs-To: \nPO-Revision-Date: 2004-10-22 16:51-0300\nLast-Translator: Diego Gil <diego@adminsa.com>\nLanguage-Team: \nLanguage: \nMIME-Version: 1.0\nContent-Type: text/plain; charset=UTF-8\nContent-Transfer-Encoding: 8bit\nX-Poedit-Language: Spanish\n";
	    t[4] = "The column index is out of range: {0}, number of columns: {1}.";
	    t[5] = "El índice de la columna está fuera de rango: {0}, número de columnas: {1}.";
	    t[12] = "Unknown Response Type {0}.";
	    t[13] = "Tipo de respuesta desconocida {0}.";
	    t[16] = "Protocol error.  Session setup failed.";
	    t[17] = "Error de protocolo. Falló el inicio de la sesión.";
	    t[20] = "The server requested password-based authentication, but no password was provided.";
	    t[21] = "El servidor requiere autenticación basada en contraseña, pero no se ha provisto ninguna contraseña.";
	    t[26] = "A result was returned when none was expected.";
	    t[27] = "Se retornó un resultado cuando no se esperaba ninguno.";
	    t[28] = "Server SQLState: {0}";
	    t[29] = "SQLState del servidor: {0}.";
	    t[30] = "The array index is out of range: {0}, number of elements: {1}.";
	    t[31] = "El índice del arreglo esta fuera de rango: {0}, número de elementos: {1}.";
	    t[32] = "Premature end of input stream, expected {0} bytes, but only read {1}.";
	    t[33] = "Final prematuro del flujo de entrada, se esperaban {0} bytes, pero solo se leyeron {1}.";
	    t[36] = "The connection attempt failed.";
	    t[37] = "El intento de conexión falló.";
	    t[38] = "Failed to create object for: {0}.";
	    t[39] = "Fallo al crear objeto: {0}.";
	    t[42] = "An error occurred while setting up the SSL connection.";
	    t[43] = "Ha ocorrido un error mientras se establecía la conexión SSL.";
	    t[48] = "No value specified for parameter {0}.";
	    t[49] = "No se ha especificado un valor para el parámetro {0}.";
	    t[50] = "The server does not support SSL.";
	    t[51] = "Este servidor no soporta SSL.";
	    t[52] = "An unexpected result was returned by a query.";
	    t[53] = "Una consulta retornó un resultado inesperado.";
	    t[60] = "Something unusual has occurred to cause the driver to fail. Please report this exception.";
	    t[61] = "Algo inusual ha ocurrido que provocó un fallo en el controlador. Por favor reporte esta excepción.";
	    t[64] = "No results were returned by the query.";
	    t[65] = "La consulta no retornó ningún resultado.";
	    table = t;
	}

	public Object handleGetObject(String msgid) throws MissingResourceException {
		int hash_val = msgid.hashCode() & 0x7fffffff;
		int idx = (hash_val % 397) << 1;
		{
			java.lang.Object found = table[idx];
			if (found == null)
				return null;
			if (msgid.equals(found))
				return table[idx + 1];
		}
		int incr = ((hash_val % 395) + 1) << 1;
		for (;;) {
			idx += incr;
			if (idx >= 794)
				idx -= 794;
			java.lang.Object found = table[idx];
			if (found == null)
				return null;
			if (msgid.equals(found))
				return table[idx + 1];
		}
	}

	public Enumeration getKeys() {
		return new Enumeration() {
			private int idx = 0;
			{
				while (idx < 794 && table[idx] == null)
					idx += 2;
			}

			public boolean hasMoreElements() {
				return (idx < 794);
			}

			public java.lang.Object nextElement() {
				java.lang.Object key = table[idx];
				do
					idx += 2;
				while (idx < 794 && table[idx] == null);
				return key;
			}
		};
	}

	public ResourceBundle getParent() {
		return parent;
	}
}
