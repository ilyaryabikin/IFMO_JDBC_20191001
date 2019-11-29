package com.efimchick.ifmo.web.jdbc;

import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

import java.sql.ResultSet;
import java.sql.SQLException;

public class RowMapperFactory {

    public RowMapper<Employee> employeeRowMapper() {
        return new RowMapper<Employee>() {
            @Override
            public Employee mapRow(ResultSet resultSet) {
                try {
                    return new Employee(resultSet.getBigDecimal("id").toBigInteger(),
                            new FullName(resultSet.getString("firstName"),
                                    resultSet.getString("lastName"),
                                    resultSet.getString("middleName")),
                            Position.valueOf(resultSet.getString("position")),
                            resultSet.getDate("hireDate").toLocalDate(),
                            resultSet.getBigDecimal("salary"));
                } catch (SQLException e) {
                    throw new UnsupportedOperationException();
                }
            }
        };
    }
}
