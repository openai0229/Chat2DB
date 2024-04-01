package ai.chat2db.server.web.api.controller.rdb.data.export.strategy;

import ai.chat2db.server.domain.api.enums.ExportFileSuffix;
import ai.chat2db.server.domain.api.param.datasource.DatabaseExportDataParam;
import ai.chat2db.spi.sql.Chat2DBContext;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.http.HttpServletResponse;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * @author: zgq
 * @date: 2024年03月24日 12:46
 */
@Getter
@AllArgsConstructor
@NoArgsConstructor
@Slf4j
public abstract class ExportDBDataStrategy {

    public String suffix;
    public String contentType;

    public void doExport(DatabaseExportDataParam param, HttpServletResponse response) {
        String databaseName = param.getDatabaseName();
        String schemaName = param.getSchemaName();
        boolean continueOnError = param.getContinueOnError();
        setResponseHeaders(param, response);

        try {
            exportDataWithConnection(databaseName, schemaName, response.getOutputStream(), continueOnError);
        } catch (Exception e) {
            log.error("Error occurred during database data export: {}", e.getMessage(), e);
            if (!continueOnError) {
                throw new RuntimeException("Error occurred during database data export", e);
            }
        }
    }

    private void exportDataWithConnection(String databaseName, String schemaName, ServletOutputStream outputStream, boolean continueOnError) throws SQLException {
        try (Connection connection = Chat2DBContext.getConnection()) {
            ZipOutputStream zipOut = new ZipOutputStream(outputStream);
            List<String> tableNames = Chat2DBContext.getMetaData().tableNames(connection, databaseName, schemaName, null);
            tableNames.addAll(Chat2DBContext.getMetaData().viewNames(connection, databaseName, schemaName));

            for (String tableName : tableNames) {
                exportTableData(connection, databaseName, schemaName, tableName, zipOut, continueOnError);
            }
        }
    }

    private void exportTableData(Connection connection, String databaseName, String schemaName, String tableName, ZipOutputStream zipOut, boolean continueOnError) throws SQLException {
        String fileName = tableName + getSuffix();
        try {
            zipOut.putNextEntry(new ZipEntry(fileName));
            try (ByteArrayOutputStream byteOut = exportData(connection, databaseName, schemaName, tableName)) {
                byteOut.writeTo(zipOut);
            }
        } catch (IOException e) {
            logAndHandleError(tableName, "exporting data", e, continueOnError);
        } finally {
            closeZipEntry(tableName, zipOut, continueOnError);
        }
    }

    private void closeZipEntry(String tableName, ZipOutputStream zipOut, boolean continueOnError) {
        try {
            zipOut.closeEntry();
        } catch (IOException e) {
            logAndHandleError(tableName, "closing zipEntry", e, continueOnError);
        }
    }

    private void logAndHandleError(String tableName, String action, Exception e, boolean continueOnError) {
        log.error("Error occurred while {} table {}: {}", action, tableName, e.getMessage(), e);
        if (!continueOnError) {
            throw new RuntimeException("Error occurred while " + action + " table " + tableName, e);
        }
    }



    private void setResponseHeaders(DatabaseExportDataParam param, HttpServletResponse response) {
        response.setContentType(contentType);
        response.setHeader("Content-disposition", "attachment;filename*=utf-8''" + getFileName(param) + ExportFileSuffix.ZIP.getSuffix());
    }

    protected String getFileName(DatabaseExportDataParam param) {
        return Objects.isNull(param.getSchemaName()) ? param.getDatabaseName() : param.getSchemaName();
    }

    protected abstract ByteArrayOutputStream exportData(Connection connection, String databaseName, String schemaName, String tableName) throws SQLException;

}