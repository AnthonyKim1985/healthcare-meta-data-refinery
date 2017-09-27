package org.bigdatacenter.refinery;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class DataRefineryDriver {
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class OperationSource {
        private Integer edl_idx;
        private Integer ecl_year;
    }

    //
    // TODO: 메타 데이터 DB의 각 컬럼별 컬럼타입을 정의한다. (0: 공통, 1: 유니크)
    //
    public static void main(String[] args) throws Exception {
        Class.forName("com.mysql.jdbc.Driver");

        try (Connection connection = DriverManager.getConnection("jdbc:mysql://dbserver.bigdatacenter.org:3306/health_care_ui?useSSL=false&useUnicode=true&characterEncoding=utf-8", "lod_ui", "lod!@")) {
            int updatedColumnCount = updateColumnTypeSetZero(connection);
            System.out.println("The column_types of " + updatedColumnCount + " columns have been updated to FALSE.");

            List<OperationSource> operationSourceList = getOperationSources(connection);

            for (OperationSource operationSource : operationSourceList) {
                List<String> columnNameList = getColumnNames(connection, operationSource);

                for (String columnName : columnNameList) {
                    if (isExclusiveColumn(connection, operationSource, columnName)) {
                        if (updateColumnType(connection, operationSource, columnName) != 1)
                            System.err.println("Not Exclusive Column: " + operationSource + ", ecl_ref=" + columnName);
                    }
                }
            }
        }
    }

    private static int updateColumnTypeSetZero(Connection connection) throws Exception {
        int updatedColumnCount;
        try (PreparedStatement preparedStatement = connection.prepareStatement("UPDATE chu_col_list_ref SET column_type = FALSE;")) {
            updatedColumnCount = preparedStatement.executeUpdate();
        }

        return updatedColumnCount;
    }

    private static int updateColumnType(Connection connection, OperationSource operationSource, String columnName) throws Exception {
        int updatedColumnCount;

        try (PreparedStatement preparedStatement = connection.prepareStatement("UPDATE chu_col_list_ref SET column_type = TRUE WHERE edl_idx = ? AND ecl_year = ? AND ecl_ref = ?;")) {
            preparedStatement.setInt(1, operationSource.getEdl_idx());
            preparedStatement.setInt(2, operationSource.getEcl_year());
            preparedStatement.setString(3, columnName);

            updatedColumnCount = preparedStatement.executeUpdate();
        }

        return updatedColumnCount;
    }

    private static boolean isExclusiveColumn(Connection connection, OperationSource operationSource, String columnName) throws Exception {
        int rowCount = 0;

        try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM chu_col_list_ref WHERE edl_idx = ? AND ecl_year = ? AND ecl_ref = ?;")) {
            preparedStatement.setInt(1, operationSource.getEdl_idx());
            preparedStatement.setInt(2, operationSource.getEcl_year());
            preparedStatement.setString(3, columnName);

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next())
                    rowCount++;
            }
        }

        switch (rowCount) {
            case 0:
                System.err.println(operationSource + ", ecl_ref = " + columnName);
                throw new NullPointerException("rowCount is zero.");
            case 1:
                return true;
            default:
                return false;
        }
    }

    private static List<String> getColumnNames(Connection connection, OperationSource operationSource) throws Exception {
        List<String> columnNameList = new ArrayList<>();

        try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT DISTINCT ecl_ref FROM chu_col_list_ref WHERE edl_idx = ? AND ecl_year = ?;")) {
            preparedStatement.setInt(1, operationSource.getEdl_idx());
            preparedStatement.setInt(2, operationSource.getEcl_year());

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next())
                    columnNameList.add(resultSet.getString("ecl_ref"));
            }
        }

        return columnNameList;
    }


    private static List<OperationSource> getOperationSources(Connection connection) throws Exception {
        List<OperationSource> operationSourceList = new ArrayList<>();

        try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT DISTINCT edl_idx, ecl_year FROM chu_col_list_ref ORDER BY edl_idx;");
             ResultSet resultSet = preparedStatement.executeQuery()) {

            while (resultSet.next()) {
                OperationSource operationSource = new OperationSource(resultSet.getInt("edl_idx"), resultSet.getInt("ecl_year"));
                operationSourceList.add(operationSource);
            }
        }

        return operationSourceList;
    }
}
