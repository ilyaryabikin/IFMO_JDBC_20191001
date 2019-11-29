package com.efimchick.ifmo.web.jdbc.service;

import com.efimchick.ifmo.web.jdbc.ConnectionSource;
import com.efimchick.ifmo.web.jdbc.domain.Department;
import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ServiceFactory {

    public EmployeeService employeeService() {
        return new EmployeeService() {
            @Override
            public List<Employee> getAllSortByHireDate(Paging paging) {
                String query = "SELECT a.*, b.name, b.location FROM employee a LEFT OUTER JOIN department b ON (a.department = b.id) ORDER BY a.hiredate";
                try (Connection connection = ConnectionSource.instance().createConnection()) {
                    ResultSet resultSet = getResultByQuery(connection, query);
                    List<Employee> employees = getEmployeeListByResult(resultSet);
                    return listByPaging(employees, paging);
                } catch (SQLException e) {
                    throw new UnsupportedOperationException();
                }
            }

            @Override
            public List<Employee> getAllSortByLastname(Paging paging) {
                String query = "SELECT a.*, b.name, b.location FROM employee a LEFT OUTER JOIN department b ON (a.department = b.id) ORDER BY a.lastName";
                try (Connection connection = ConnectionSource.instance().createConnection()) {
                    ResultSet resultSet = getResultByQuery(connection, query);
                    List<Employee> employees = getEmployeeListByResult(resultSet);
                    return listByPaging(employees, paging);
                } catch (SQLException e) {
                    throw new UnsupportedOperationException();
                }
            }

            @Override
            public List<Employee> getAllSortBySalary(Paging paging) {
                String query = "SELECT a.*, b.name, b.location FROM employee a LEFT OUTER JOIN department b ON (a.department = b.id) ORDER BY a.salary";
                try (Connection connection = ConnectionSource.instance().createConnection()) {
                    ResultSet resultSet = getResultByQuery(connection, query);
                    List<Employee> employees = getEmployeeListByResult(resultSet);
                    return listByPaging(employees, paging);
                } catch (SQLException e) {
                    throw new UnsupportedOperationException();
                }
            }

            @Override
            public List<Employee> getAllSortByDepartmentNameAndLastname(Paging paging) {
                String query = "SELECT a.*, b.name, b.location FROM employee a LEFT OUTER JOIN department b ON (a.department = b.id) ORDER BY a.department, a.lastName";
                try (Connection connection = ConnectionSource.instance().createConnection()) {
                    ResultSet resultSet = getResultByQuery(connection, query);
                    List<Employee> employees = getEmployeeListByResult(resultSet);
                    return listByPaging(employees, paging);
                } catch (SQLException e) {
                    throw new UnsupportedOperationException();
                }
            }

            @Override
            public List<Employee> getByDepartmentSortByHireDate(Department department, Paging paging) {
                String query = "SELECT a.*, b.name, b.location FROM employee a LEFT OUTER JOIN department b ON (a.department = b.id) ORDER BY a.hireDate";
                try (Connection connection = ConnectionSource.instance().createConnection()) {
                    ResultSet resultSet = getResultByQuery(connection, query);
                    List<Employee> employees = getEmployeeListByResult(resultSet);
                    employees = filterEmployeesByDepartment(employees, department);
                    return listByPaging(employees, paging);
                } catch (SQLException e) {
                    throw new UnsupportedOperationException();
                }
            }

            @Override
            public List<Employee> getByDepartmentSortBySalary(Department department, Paging paging) {
                String query = "SELECT a.*, b.name, b.location FROM employee a LEFT OUTER JOIN department b ON (a.department = b.id) ORDER BY a.salary";
                try (Connection connection = ConnectionSource.instance().createConnection()) {
                    ResultSet resultSet = getResultByQuery(connection, query);
                    List<Employee> employees = getEmployeeListByResult(resultSet);
                    employees = filterEmployeesByDepartment(employees, department);
                    return listByPaging(employees, paging);
                } catch (SQLException e) {
                    throw new UnsupportedOperationException();
                }
            }

            @Override
            public List<Employee> getByDepartmentSortByLastname(Department department, Paging paging) {
                String query = "SELECT a.*, b.name, b.location FROM employee a LEFT OUTER JOIN department b ON (a.department = b.id) ORDER BY a.lastName";
                try (Connection connection = ConnectionSource.instance().createConnection()) {
                    ResultSet resultSet = getResultByQuery(connection, query);
                    List<Employee> employees = getEmployeeListByResult(resultSet);
                    employees = filterEmployeesByDepartment(employees, department);
                    return listByPaging(employees, paging);
                } catch (SQLException e) {
                    throw new UnsupportedOperationException();
                }
            }

            @Override
            public List<Employee> getByManagerSortByLastname(Employee manager, Paging paging) {
                String query = "SELECT a.*, b.name, b.location FROM employee a LEFT OUTER JOIN department b ON (a.department = b.id) ORDER BY a.lastName";
                try (Connection connection = ConnectionSource.instance().createConnection()) {
                    ResultSet resultSet = getResultByQuery(connection, query);
                    List<Employee> employees = getEmployeeListByResult(resultSet);
                    employees = filterEmployeesByManager(employees, manager);
                    return listByPaging(employees, paging);
                } catch (SQLException e) {
                    throw new UnsupportedOperationException();
                }
            }

            @Override
            public List<Employee> getByManagerSortByHireDate(Employee manager, Paging paging) {
                String query = "SELECT a.*, b.name, b.location FROM employee a LEFT OUTER JOIN department b ON (a.department = b.id) ORDER BY a.hireDate";
                try (Connection connection = ConnectionSource.instance().createConnection()) {
                    ResultSet resultSet = getResultByQuery(connection, query);
                    List<Employee> employees = getEmployeeListByResult(resultSet);
                    employees = filterEmployeesByManager(employees, manager);
                    return listByPaging(employees, paging);
                } catch (SQLException e) {
                    throw new UnsupportedOperationException();
                }
            }

            @Override
            public List<Employee> getByManagerSortBySalary(Employee manager, Paging paging) {
                String query = "SELECT a.*, b.name, b.location FROM employee a LEFT OUTER JOIN department b ON (a.department = b.id) ORDER BY a.salary";
                try (Connection connection = ConnectionSource.instance().createConnection()) {
                    ResultSet resultSet = getResultByQuery(connection, query);
                    List<Employee> employees = getEmployeeListByResult(resultSet);
                    employees = filterEmployeesByManager(employees, manager);
                    return listByPaging(employees, paging);
                } catch (SQLException e) {
                    throw new UnsupportedOperationException();
                }
            }

            @Override
            public Employee getWithDepartmentAndFullManagerChain(Employee employee) {
                String query = "SELECT a.*, b.name, b.location FROM employee a LEFT OUTER JOIN department b ON (a.department = b.id)";
                try (Connection connection = ConnectionSource.instance().createConnection()) {
                    ResultSet resultSet = getResultByQuery(connection, query);
                    while (resultSet.next()) {
                        if (getBigInteger(resultSet, "id").equals(employee.getId())) {
                            return getEmployeeHierarchy(resultSet);
                        }
                    }
                    return null;
                } catch (SQLException e) {
                    throw new UnsupportedOperationException();
                }
            }

            @Override
            public Employee getTopNthBySalaryByDepartment(int salaryRank, Department department) {
                String query = "SELECT a.*, b.name, b.location FROM employee a LEFT OUTER JOIN department b ON (a.department = b.id) ORDER BY a.salary DESC";
                try (Connection connection = ConnectionSource.instance().createConnection()) {
                    ResultSet resultSet = getResultByQuery(connection, query);
                    List<Employee> employees = getEmployeeListByResult(resultSet);
                    employees = filterEmployeesByDepartment(employees, department);
                    return employees.get(salaryRank - 1);
                } catch (SQLException e) {
                    throw new UnsupportedOperationException();
                }
            }

            private ResultSet getResultByQuery(Connection connection, String query) throws SQLException {
                return connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).executeQuery(query);
            }

            private List<Employee> listByPaging(List<Employee> employees, Paging paging) {
                int lowerBound = (paging.page - 1) * paging.itemPerPage;
                int upperBound = Math.min(paging.page * paging.itemPerPage, employees.size());
                return employees.subList(lowerBound, upperBound);
            }

            private BigInteger getBigInteger(ResultSet resultSet, String column) throws SQLException {
                return resultSet.getBigDecimal(column).toBigInteger();
            }

            private Employee getEmployeesManager(ResultSet resultSet) throws SQLException {
                Employee manager = null;
                resultSet.getString("manager");
                if (!resultSet.wasNull()) {
                    BigInteger managerId = getBigInteger(resultSet, "manager");
                    int currentCursor = resultSet.getRow();
                    resultSet.beforeFirst();
                    while (resultSet.next()) {
                        if (getBigInteger(resultSet, "id").equals(managerId)) {
                            manager = new Employee(getBigInteger(resultSet, "id"),
                                    new FullName(resultSet.getString("firstName"),
                                            resultSet.getString("lastName"),
                                            resultSet.getString("middleName")),
                                    Position.valueOf(resultSet.getString("position")),
                                    resultSet.getDate("hireDate").toLocalDate(),
                                    resultSet.getBigDecimal("salary"),
                                    null,
                                    getEmployeesDepartment(resultSet));
                            break;
                        }
                    }
                    resultSet.absolute(currentCursor);
                }
                return manager;
            }

            private Employee getEmployeeHierarchy(ResultSet resultSet) throws SQLException {
                Employee manager = null;
                resultSet.getString("manager");
                if (!resultSet.wasNull()) {
                    int currentCursor = resultSet.getRow();
                    BigInteger managerId = getBigInteger(resultSet, "manager");
                    resultSet.beforeFirst();
                    while (resultSet.next()) {
                        if (getBigInteger(resultSet, "id").equals(managerId)) {
                            manager = getEmployeeHierarchy(resultSet);
                        }
                    }
                    resultSet.absolute(currentCursor);
                }
                return new Employee(getBigInteger(resultSet, "id"),
                        new FullName(resultSet.getString("firstName"),
                                resultSet.getString("lastName"),
                                resultSet.getString("middleName")),
                        Position.valueOf(resultSet.getString("position")),
                        resultSet.getDate("hireDate").toLocalDate(),
                        resultSet.getBigDecimal("salary"),
                        manager,
                        getEmployeesDepartment(resultSet));
            }

            private Department getEmployeesDepartment(ResultSet resultSet) throws SQLException {
                Department department = null;
                resultSet.getString("department");
                if (!resultSet.wasNull()) {
                    department = getDepartmentByResult(resultSet);
                }
                return department;
            }

            private Employee getEmployeeByResult(ResultSet resultSet) throws SQLException {
                return new Employee(getBigInteger(resultSet, "id"),
                        new FullName(resultSet.getString("firstName"),
                                resultSet.getString("lastName"),
                                resultSet.getString("middleName")),
                        Position.valueOf(resultSet.getString("position")),
                        resultSet.getDate("hireDate").toLocalDate(),
                        resultSet.getBigDecimal("salary"),
                        getEmployeesManager(resultSet),
                        getEmployeesDepartment(resultSet));
            }

            private Department getDepartmentByResult(ResultSet resultSet) throws SQLException {
                return new Department(getBigInteger(resultSet, "department"),
                        resultSet.getString("name"),
                        resultSet.getString("location"));
            }

            private List<Employee> getEmployeeListByResult(ResultSet resultSet) throws SQLException {
                List<Employee> employees = new ArrayList<>();
                while (resultSet.next()) {
                    employees.add(getEmployeeByResult(resultSet));
                }
                return employees;
            }

            private List<Employee> filterEmployeesByManager(List<Employee> employees, Employee manager) {z
                return employees.stream()
                        .filter(employee -> employee.getManager() != null)
                        .filter(employee -> employee.getManager().getId().equals(manager.getId()))
                        .collect(Collectors.toList());
            }

            private List<Employee> filterEmployeesByDepartment(List<Employee> employees, Department department) {
                return employees.stream()
                        .filter(employee -> employee.getDepartment() != null)
                        .filter(employee -> employee.getDepartment().getId().equals(department.getId()))
                        .collect(Collectors.toList());
            }
        };
    }
}