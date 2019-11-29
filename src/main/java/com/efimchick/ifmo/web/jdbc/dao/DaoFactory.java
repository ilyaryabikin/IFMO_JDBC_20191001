package com.efimchick.ifmo.web.jdbc.dao;

import com.efimchick.ifmo.web.jdbc.ConnectionSource;
import com.efimchick.ifmo.web.jdbc.domain.Department;
import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class DaoFactory {
    public EmployeeDao employeeDAO() {
        return new EmployeeDao() {
            @Override
            public List<Employee> getByDepartment(Department department) {
                try (Connection connection = ConnectionSource.instance().createConnection()) {
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery("SELECT * FROM employee WHERE department = " + department.getId());
                    List<Employee> employees = new ArrayList<>();

                    while (resultSet.next()) {
                        employees.add(getEmployeeByResult(resultSet));
                    }

                    return employees;
                } catch (SQLException e) {
                    throw new UnsupportedOperationException();
                }
            }

            @Override
            public List<Employee> getByManager(Employee employee) {
                try (Connection connection = ConnectionSource.instance().createConnection()) {
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery("SELECT * FROM employee WHERE manager = " + employee.getId());
                    List<Employee> employees = new ArrayList<>();

                    while (resultSet.next()) {
                        employees.add(getEmployeeByResult(resultSet));
                    }

                    return employees;
                } catch (SQLException e) {
                    throw new UnsupportedOperationException();
                }
            }

            @Override
            public Optional<Employee> getById(BigInteger Id) {
                try (Connection connection = ConnectionSource.instance().createConnection()) {
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery("SELECT * FROM employee WHERE id = " + Id);

                    if (resultSet.next()) {
                        return Optional.of(getEmployeeByResult(resultSet));
                    } else {
                        return Optional.empty();
                    }
                } catch (SQLException e) {
                    throw new UnsupportedOperationException();
                }
            }

            @Override
            public List<Employee> getAll() {
                try (Connection connection = ConnectionSource.instance().createConnection()) {
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery("SELECT * FROM employee");
                    List<Employee> employees = new ArrayList<>();
                    while (resultSet.next()) {
                        employees.add(getEmployeeByResult(resultSet));
                    }

                    return employees;
                } catch (SQLException e) {
                    throw new UnsupportedOperationException();
                }
            }

            @Override
            public Employee save(Employee employee) {
                try (Connection connection = ConnectionSource.instance().createConnection()) {
                    Statement statement = connection.createStatement();
                    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd-MM-YYYY");

                    if (!getById(employee.getId()).isPresent()) {
                        statement.executeUpdate("INSERT INTO EMPLOYEE VALUES " +
                                "(" + employee.getId() +
                                ", '" + employee.getFullName().getFirstName() +
                                "', '" + employee.getFullName().getLastName() +
                                "', '" + employee.getFullName().getMiddleName() +
                                "','" + employee.getPosition() +
                                "', " + employee.getManagerId() +
                                ", TO_DATE('" + employee.getHired().format(dateTimeFormatter) + "', 'DD-MM-YYYY')" +
                                ", " + employee.getSalary() +
                                ", " + employee.getDepartmentId() + ")");
                    } else {
                        statement.executeUpdate("UPDATE employee SET" +
                                "firstname = '" + employee.getFullName().getFirstName() +
                                "', lastname = '" + employee.getFullName().getLastName() +
                                "', middlename = '" + employee.getFullName().getMiddleName() +
                                "', position = '" + employee.getPosition() +
                                "', manager = " + employee.getManagerId() +
                                ", hiredate = TO_DATE('" + employee.getHired().format(dateTimeFormatter) + "', 'DD-MM-YYYY')" +
                                ", salary = " + employee.getSalary() +
                                ", department = " + employee.getDepartmentId() + "WHERE id = " + employee.getId());
                    }
                    return employee;
                } catch (SQLException e) {
                    throw new UnsupportedOperationException();
                }
            }

            @Override
            public void delete(Employee employee) {
                try (Connection connection = ConnectionSource.instance().createConnection()) {
                    Statement statement = connection.createStatement();
                    statement.executeUpdate("DELETE FROM employee WHERE id = " + employee.getId());
                } catch (SQLException e) {
                    throw new UnsupportedOperationException();
                }
            }
        };
    }

    public DepartmentDao departmentDAO() {
        return new DepartmentDao() {
            @Override
            public Optional<Department> getById(BigInteger Id) {
                try (Connection connection = ConnectionSource.instance().createConnection()) {
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery("SELECT * FROM department WHERE id = " + Id);

                    if (resultSet.next()) {
                        return Optional.of(getDepartmentByResult(resultSet));
                    } else {
                        return Optional.empty();
                    }
                } catch (SQLException e) {
                    throw new UnsupportedOperationException();
                }
            }

            @Override
            public List<Department> getAll() {
                try (Connection connection = ConnectionSource.instance().createConnection()) {
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery("SELECT * FROM department");
                    List<Department> departments = new ArrayList<>();

                    while (resultSet.next()) {
                        departments.add(getDepartmentByResult(resultSet));
                    }

                    return departments;
                } catch (SQLException e) {
                    throw new UnsupportedOperationException();
                }
            }

            @Override
            public Department save(Department department) {
                try (Connection connection = ConnectionSource.instance().createConnection()) {
                    Statement statement = connection.createStatement();
                    if (!getById(department.getId()).isPresent()) {
                        statement.executeUpdate("INSERT INTO department VALUES " +
                                "(" + department.getId() +
                                ", '" + department.getName() +
                                "', '" + department.getLocation() + "')");
                    } else {
                        statement.executeUpdate("UPDATE department SET name = '" + department.getName() + "', location = '" + department.getLocation() + "' WHERE id = " + department.getId());
                    }
                    return department;
                } catch (SQLException e) {
                    throw new UnsupportedOperationException();
                }
            }

            @Override
            public void delete(Department department) {
                try (Connection connection = ConnectionSource.instance().createConnection()) {
                    Statement statement = connection.createStatement();
                    statement.executeUpdate("DELETE FROM department WHERE id = " + department.getId());
                } catch (SQLException e) {
                    throw new UnsupportedOperationException();
                }
            }
        };
    }

    private Employee getEmployeeByResult(ResultSet resultSet) throws SQLException {
        BigInteger id = resultSet.getBigDecimal("id").toBigInteger();
        FullName fullName = new FullName(resultSet.getString("firstName"),
                resultSet.getString("lastName"),
                resultSet.getString("middleName"));
        Position position = Position.valueOf(resultSet.getString("position"));
        LocalDate hiredDate = resultSet.getDate("hireDate").toLocalDate();
        BigDecimal salary = resultSet.getBigDecimal("salary");
        String managerId = resultSet.getString("manager");
        if (resultSet.wasNull()) {
            managerId = "0";
        }
        String departmentId = resultSet.getString("department");
        if (resultSet.wasNull()) {
            departmentId = "0";
        }
        return new Employee(id, fullName, position, hiredDate, salary, new BigInteger(managerId), new BigInteger(departmentId));
    }

    private Department getDepartmentByResult(ResultSet resultSet) throws SQLException {
        BigInteger id = resultSet.getBigDecimal("id").toBigInteger();
        String fullName = resultSet.getString("name");
        String location = resultSet.getString("location");
        return new Department(id, fullName, location);
    }
}