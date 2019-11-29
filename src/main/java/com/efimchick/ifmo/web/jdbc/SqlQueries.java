package com.efimchick.ifmo.web.jdbc;

/**
 * Implement sql queries like described
 */
public class SqlQueries {
    //Select all employees sorted by last name in ascending order
    //language=HSQLDB
    String select01 = "SELECT * FROM employee ORDER BY lastname";

    //Select employees having no more than 5 characters in last name sorted by last name in ascending order
    //language=HSQLDB
    String select02 = "SELECT * FROM employee WHERE LENGTH(lastname) <= 5 ORDER BY lastname";

    //Select employees having salary no less than 2000 and no more than 3000
    //language=HSQLDB
    String select03 = "SELECT * FROM employee WHERE salary >= 2000 AND salary <= 3000";

    //Select employees having salary no more than 2000 or no less than 3000
    //language=HSQLDB
    String select04 = "SELECT * FROM employee WHERE salary <= 2000 OR salary >= 3000";

    //Select employees assigned to a department and corresponding department name
    //language=HSQLDB
    String select05 = "SELECT e.*, d.name FROM employee AS e, department AS d WHERE e.department = d.id";

    //Select all employees and corresponding department name if there is one.
    //Name column containing name of the department "depname".
    //language=HSQLDB
    String select06 = "SELECT e.*, d.name AS depname FROM employee AS e LEFT JOIN department AS d ON e.department = d.id";

    //Select total salary pf all employees. Name it "total".
    //language=HSQLDB
    String select07 = "SELECT SUM(salary) AS total FROM employee";

    //Select all departments and amount of employees assigned per department
    //Name column containing name of the department "depname".
    //Name column containing employee amount "staff_size".
    //language=HSQLDB
    String select08 = "SELECT d.name AS depname, COUNT(e.id) AS staff_size FROM department AS d, employee AS e WHERE d.id = e.department GROUP BY depname";

    //Select all departments and values of total and average salary per department
    //Name column containing name of the department "depname".
    //language=HSQLDB
    String select09 = "SELECT d.name AS depname, SUM(e.salary) AS total, AVG(e.salary) AS average FROM department AS d, employee AS e WHERE d.id = e.department GROUP BY depname";

    //Select all employees and their managers if there is one.
    //Name column containing employee lastname "employee".
    //Name column containing manager lastname "manager".
    //language=HSQLDB
    String select10 = "SELECT e1.lastname AS employee, e2.lastname AS manager FROM employee AS e1 LEFT JOIN employee AS e2 ON e1.manager = e2.id";
}
