package ai.chat2db.plugin.oracle.value.sub;

import ai.chat2db.plugin.oracle.value.template.OracleDmlValueTemplate;
import ai.chat2db.spi.jdbc.DefaultValueProcessor;
import ai.chat2db.spi.model.JDBCDataValue;
import ai.chat2db.spi.model.SQLDataValue;

/**
 * @author: zgq
 * @date: 2024年06月21日 12:55
 */
public class OracleXmlValueProcessor extends DefaultValueProcessor {

    @Override
    public String convertSQLValueByType(SQLDataValue dataValue) {
        return wrap(dataValue.getValue());
    }


    @Override
    public String convertJDBCValueByType(JDBCDataValue dataValue) {
        return dataValue.getString();
    }


    @Override
    public String convertJDBCValueStrByType(JDBCDataValue dataValue) {
        return wrap(dataValue.getString());
    }

    private String wrap(String value) {
        return String.format(OracleDmlValueTemplate.XML_TEMPLATE, value);
    }
}
