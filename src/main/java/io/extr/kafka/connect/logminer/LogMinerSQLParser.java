package io.extr.kafka.connect.logminer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;

public class LogMinerSQLParser {
	private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerSQLParser.class);

	public static Map<String, Map<String, String>> parseRedoSQL(String redoSQL) throws JSQLParserException {
		LOGGER.trace("Parsing redo SQL into column string map");
		String cleanRedoSQL = redoSQL.replace("IS NULL", "= NULL");
		Statement stmt = CCJSqlParserUtil.parse(cleanRedoSQL);
		final LinkedHashMap<String, String> after = new LinkedHashMap<>();
		final LinkedHashMap<String, String> before = new LinkedHashMap<>();
		final Map<String, Map<String, String>> changes = new HashMap<>();

		if (stmt instanceof Insert) {
			Insert insert = (Insert) stmt;

			for (Column c : insert.getColumns()) {
				after.put(cleanString(c.getColumnName()), null);
			}

			ExpressionList eList = (ExpressionList) insert.getItemsList();
			List<Expression> valueList = eList.getExpressions();
			int i = 0;
			for (String key : after.keySet()) {
				String value = cleanString(valueList.get(i).toString());
				after.put(key, value);
				i++;
			}

		} else if (stmt instanceof Update) {
			Update update = (Update) stmt;
			for (Column c : update.getColumns()) {
				after.put(cleanString(c.getColumnName()), null);
			}

			Iterator<Expression> it = update.getExpressions().iterator();
			for (String key : after.keySet()) {
				Object o = it.next();
				String value = cleanString(o.toString());
				after.put(key, value);
			}

			update.getWhere().accept(new ExpressionVisitorAdapter() {
				@Override
				public void visit(final EqualsTo expr) {
					String col = cleanString(expr.getLeftExpression().toString());
					String value = cleanString(expr.getRightExpression().toString());
					before.put(col, value);
				}
			});

		} else if (stmt instanceof Delete) {
			Delete delete = (Delete) stmt;
			delete.getWhere().accept(new ExpressionVisitorAdapter() {
				@Override
				public void visit(final EqualsTo expr) {
					String col = cleanString(expr.getLeftExpression().toString());
					String value = cleanString(expr.getRightExpression().toString());
					before.put(col, value);
				}
			});
		}

		changes.put(LogMinerSourceConnectorConstants.FIELD_BEFORE_DATA_ROW, before);
		changes.put(LogMinerSourceConnectorConstants.FIELD_AFTER_DATA_ROW, after);

		LOGGER.trace("Parsed redo SQL into before: {}, after {}", before, after);
		return changes;
	}

	private static String cleanString(String str) {
		if (str.startsWith("TIMESTAMP"))
			str = str.replace("TIMESTAMP ", "");
		if (str.startsWith("'") && str.endsWith("'"))
			str = str.substring(1, str.length() - 1);
		if (str.startsWith("\"") && str.endsWith("\""))
			str = str.substring(1, str.length() - 1);
		return str.replace("IS NULL", "= NULL").trim();
	}

}
