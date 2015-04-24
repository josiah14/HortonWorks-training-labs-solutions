CREATE DATABASE mydb;

USE mydb;

CREATE TABLE employees (
    id INT NOT NULL AUTO_INCREMENT,
    firstName VARCHAR(30),
    lastName VARCHAR(30),
    department VARCHAR(30),
    salary DOUBLE,
    PRIMARY KEY (id)
);

INSERT INTO employees(firstName, lastName, department, salary) VALUES ('Mark', 'Twain', 'Training', 50000.00), ('Samuel', 'Clemens', 'HR', 40000.00),('Tom', 'Sawyer', 'Training', 30000.00);

CREATE TABLE salarydata (
    id INT NOT NULL AUTO_INCREMENT,
    department VARCHAR(30),
    salary DOUBLE,
    mapkey INT,
    PRIMARY KEY (id)
);