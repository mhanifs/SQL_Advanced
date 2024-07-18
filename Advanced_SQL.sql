--This practice includes Complex SQL queries, JSON functionalities, Window functions, Groups/ Island concept for identifying consecutive number/ days/ date and more.

SELECT * FROM Transactions;

WITH cte AS
(
SELECT *,
DENSE_RANK() OVER(PARTITION BY user_id ORDER BY transaction_date ASC) r,
SUM(amount) OVER(PARTITION BY user_id, transaction_date ORDER BY transaction_date DESC) t_amounts,
DATE_SUB(transaction_date, INTERVAL ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY transaction_date)DAY) as dates
FROM Transactions
)
SELECT user_id, SUM(t_amounts) AS total_amount, dates AS transaction_date
FROM cte
WHERE r = 1
GROUP BY user_id, dates
ORDER BY total_amount DESC
LIMIT 1;

WITH daily_totals AS (
    SELECT 
        user_id, 
        transaction_date, 
        SUM(amount) AS total_amount
    FROM 
        Transactions
    GROUP BY 
        user_id, 
        transaction_date
)
,
ranked_totals AS (
    SELECT 
        user_id, 
        transaction_date, 
        total_amount,
        ROW_NUMBER() OVER (ORDER BY total_amount DESC, transaction_date ASC) AS rnk
    FROM 
        daily_totals
)
SELECT 
    user_id, 
    total_amount, 
    transaction_date
FROM 
    ranked_totals
WHERE 
    rnk = 1;

show tables;

-- Create the Employee table
CREATE TABLE Emp (
    Id INT PRIMARY KEY,
    Name VARCHAR(50)
);

-- Insert sample data into the Employee table
INSERT INTO Emp (Id, Name) VALUES
(1, 'John'),
(2, 'Jane'),
(3, 'Alice'),
(4, 'Bob'),
(5, 'Carol');

-- Create the Transfers table
CREATE TABLE Transfers (
    employee_id INT,
    from_dept VARCHAR(50),
    to_dept VARCHAR(50),
    transfer_date DATE,
    FOREIGN KEY (employee_id) REFERENCES Employee(Id)
);

-- Insert sample data into the Transfers table
INSERT INTO Transfers (employee_id, from_dept, to_dept, transfer_date) VALUES
(1, 'A', 'B', '2023-01-01'),
(2, 'A', 'C', '2023-01-02'),
(1, 'B', 'A', '2023-01-03'),
(3, 'A', 'B', '2023-01-04'),
(2, 'C', 'A', '2023-01-05'),
(1, 'A', 'C', '2023-01-06'),
(4, 'B', 'C', '2023-01-07'),
(3, 'B', 'A', '2023-01-08'),
(1, 'C', 'B', '2023-01-09');

SELECT * From Transfers;

WITH cte AS
(
SELECT t.employee_id AS ID, e.name AS Name, COUNT(from_dept) AS transfer_count
FROM Transfers t
JOIN Emp e
ON t.employee_id = e.id
GROUP BY employee_id
),
ranking AS
(
SELECT *,
RANK() OVER(ORDER BY transfer_count DESC) r
FROM cte
)
SELECT ID, Name, transfer_count
FROM ranking
WHERE r = 1;


-- Create the Attendance table
CREATE TABLE Attendance (
    employee_id INT,
    attendance_date DATE,
    status VARCHAR(10)
);

-- Insert sample data into the Attendance table
INSERT INTO Attendance (employee_id, attendance_date, status) VALUES
(1, '2023-01-01', 'Present'),
(1, '2023-01-02', 'Absent'),
(1, '2023-01-03', 'Absent'),
(1, '2023-01-04', 'Absent'),
(1, '2023-01-05', 'Present'),
(2, '2023-01-01', 'Absent'),
(2, '2023-01-02', 'Absent'),
(2, '2023-01-03', 'Present'),
(2, '2023-01-04', 'Absent'),
(3, '2023-01-01', 'Absent'),
(3, '2023-01-02', 'Absent'),
(3, '2023-01-03', 'Absent'),
(3, '2023-01-04', 'Absent'),
(3, '2023-01-05', 'Absent');

SELECT * FROM Attendance;
WITH cte AS
(
SELECT employee_id, attendance_date, status,
DATE_SUB(attendance_date, INTERVAL ROW_NUMBER() OVER(PARTITION BY employee_id, status ORDER BY attendance_date) DAY) AS dates_group
FROM Attendance
WHERE status = 'absent'
)
,

grouped_dates AS 
(
SELECT employee_id,
MIN(attendance_date) AS start_date,
MAX(attendance_date) AS end_date,
COUNT(*) AS total_days
FROM cte
GROUP BY employee_id, dates_group
)
SELECT employee_id, start_date, end_date
FROM grouped_dates
WHERE total_days >=3;

CREATE TABLE Emp_1 (
    id INT,
    name VARCHAR(50),
    salary DECIMAL(10, 2),
    department_id INT
);

INSERT INTO Emp_1 (id, name, salary, department_id) VALUES
(1, 'Alice', 90000, 1),
(2, 'Bob', 85000, 1),
(3, 'Charlie', 90000, 1),
(4, 'David', 80000, 2),
(5, 'Eve', 95000, 2),
(6, 'Frank', 95000, 2);

SELECT * FROM Emp_1;

WITH cte AS
(
SELECT id, name, salary, department_id,
DENSE_RANK() OVER(PARTITION BY department_id ORDER BY salary DESC) r
FROM Emp_1
)
SELECT department_id, name AS employee_name, salary
FROM cte
WHERE r = 1;

show tables;

CREATE TABLE Users (
    user_id INT,
    user_name VARCHAR(50)
);

CREATE TABLE Activity (
    activity_id INT,
    user_id INT,
    activity_date DATE
);

INSERT INTO Users (user_id, user_name) VALUES
(1, 'Alice'),
(2, 'Bob'),
(3, 'Charlie'),
(4, 'David'),
(5, 'Eve');

INSERT INTO Activity (activity_id, user_id, activity_date) VALUES
(1, 1, '2024-04-15'),
(2, 2, '2024-05-20'),
(3, 1, '2024-06-05'),
(4, 3, '2024-06-10'),
(5, 4, '2024-04-01'),
(6, 5, '2024-05-30');


SELECT * FROM Activity;

SELECT DISTINCT(a.user_id) AS user_id, u.user_name
FROM Activity A
LEFT JOIN Users u
USING (user_id)
WHERE MONTH(a.activity_date) <> '05'AND YEAR(a.activity_date) = '2024';

EXPLAIN WITH users_with_may_activity AS (
    SELECT DISTINCT user_id
    FROM Activity
    WHERE MONTH(activity_date) = 5 AND YEAR(activity_date) = 2024
)

SELECT u.user_id, u.user_name
FROM Users u
LEFT JOIN users_with_may_activity uwma
ON u.user_id = uwma.user_id
WHERE uwma.user_id IS NULL;

CREATE TABLE Products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(100),
    price_per_unit DECIMAL(10, 2)
);

CREATE TABLE Orders1 (
    order_id INT PRIMARY KEY,
    order_date DATE,
    customer_id INT
);

CREATE TABLE Order_Items (
    order_item_id INT PRIMARY KEY,
    order_id INT,
    product_id INT,
    quantity INT
);

INSERT INTO Products (product_id, product_name, price_per_unit) VALUES
(1, 'Product A', 10.00),
(2, 'Product B', 20.00),
(3, 'Product C', 30.00);

INSERT INTO Orders1 (order_id, order_date, customer_id) VALUES
(1, '2024-06-01', 1),
(2, '2024-06-02', 2),
(3, '2024-06-03', 3);

INSERT INTO Order_Items (order_item_id, order_id, product_id, quantity) VALUES
(1, 1, 1, 2),
(2, 1, 2, 1),
(3, 2, 1, 1),
(4, 2, 3, 5),
(5, 3, 2, 2),
(6, 3, 3, 1);

SELECT * FROM Order_Items;

WITH cte AS
(
SELECT order_id, product_id, SUM(quantity) AS total_qauntities
FROM Order_Items
GROUP BY product_id, order_id
)
SELECT p.product_id, p.product_name, c.total_qauntities * p.price_per_unit/ c.total_qauntities AS avg_sales_per_order 
FROM Products p
LEFT JOIN CTE c
USING (product_id)
GROUP BY product_id, order_id
ORDER BY avg_sales_per_order DESC
LIMIT 1;

-- Create the Products table
CREATE TABLE Products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(50),
    price_per_unit DECIMAL(10, 2)
);

-- Insert data into the Products table
INSERT INTO Products (product_id, product_name, price_per_unit) VALUES
(1, 'Product A', 10),
(2, 'Product B', 20),
(3, 'Product C', 30);

-- Create the Order_Items table
CREATE TABLE Order_Items (
    order_id INT,
    product_id INT,
    quantity INT,
    PRIMARY KEY (order_id, product_id),
    FOREIGN KEY (product_id) REFERENCES Products(product_id)
);

-- Insert data into the Order_Items table
INSERT INTO Order_Items (order_id, product_id, quantity) VALUES
(1, 1, 2),
(2, 1, 3),
(3, 1, 1),
(4, 2, 1),
(5, 2, 4),
(6, 3, 5),
(7, 3, 2),
(8, 3, 3);

SELECT * FROM Products;

WITH cte AS
(
SELECT order_id, product_id, SUM(quantity) AS total_qauntities
FROM Order_Items
GROUP BY product_id, order_id
)
SELECT p.product_id, p.product_name, c.total_qauntities * p.price_per_unit/ c.total_qauntities AS avg_sales_per_order 
FROM Products p
LEFT JOIN CTE c
USING (product_id)
GROUP BY product_id, order_id
ORDER BY avg_sales_per_order DESC
LIMIT 1;

show databases;

use sql_with_gpt;
show tables;

CREATE TABLE Emp_2 (
    id INT PRIMARY KEY,
    name VARCHAR(50),
    department_id INT,
    manager_id INT,
    salary INT
);

INSERT INTO Emp_2 (id, name, department_id, manager_id, salary) VALUES
(1, 'John', 1, 3, 100000),
(2, 'Jane', 2, 4, 80000),
(3, 'Doe', 1, NULL, 120000),
(4, 'Alice', 2, NULL, 90000),
(5, 'Bob', 3, 6, 70000),
(6, 'Charlie', 3, NULL, 150000),
(7, 'David', 1, 3, 95000),
(8, 'Eve', 2, 4, 87000),
(9, 'Frank', 3, 6, 110000);

SELECT * FROM Emp_2;

WITH cte AS
(
SELECT department_id, AVG(salary) average_salary
FROM Emp_2
GROUP BY department_id

)
SELECT department_id, average_salary
FROM cte
WHERE average_salary = (SELECT MAX(average_salary) FROM cte);

show tables;

CREATE TABLE Emp_3 (
    emp_id INT PRIMARY KEY,
    emp_name VARCHAR(50),
    department_id INT,
    manager_id INT,
    salary INT
);

CREATE TABLE Dept_3 (
    department_id INT PRIMARY KEY,
    department_name VARCHAR(50)
);

INSERT INTO Emp_3 (emp_id, emp_name, department_id, manager_id, salary) VALUES
(1, 'John', 1, 3, 100000),
(2, 'Jane', 2, 4, 80000),
(3, 'Doe', 1, NULL, 120000),
(4, 'Alice', 2, NULL, 90000),
(5, 'Bob', 3, 6, 70000),
(6, 'Charlie', 3, NULL, 150000),
(7, 'David', 1, 3, 95000),
(8, 'Eve', 2, 4, 87000),
(9, 'Frank', 3, 6, 110000);

INSERT INTO Dept_3 (department_id, department_name) VALUES
(1, 'HR'),
(2, 'Finance'),
(3, 'Engineering');

WITH Avg_dept AS
(
SELECT department_id, AVG(salary) AS avg_salary_dept
FROM Emp_3
GROUP BY department_id
),
Avg_all AS
(
SELECT AVG(salary) AS avg_salary_all
FROM Emp_3)

SELECT d.department_name
FROM Avg_dept ad
LEFT JOIN dept_3 d
USING (department_id)
WHERE avg_salary_dept > (SELECT avg_salary_all FROM avg_all);

CREATE TABLE Emp_4 (
    emp_id INT PRIMARY KEY,
    emp_name VARCHAR(50),
    department_id INT,
    salary INT
);

CREATE TABLE Dept_4 (
    department_id INT PRIMARY KEY,
    department_name VARCHAR(50)
);

CREATE TABLE Projects_4 (
    project_id INT PRIMARY KEY,
    project_name VARCHAR(50),
    department_id INT
);

INSERT INTO Emp_4 (emp_id, emp_name, department_id, salary) VALUES
(1, 'John', 1, 100000),
(2, 'Jane', 2, 80000),
(3, 'Doe', 1, 120000),
(4, 'Alice', 2, 90000),
(5, 'Bob', 3, 70000),
(6, 'Charlie', 3, 150000),
(7, 'David', 1, 95000),
(8, 'Eve', 2, 87000),
(9, 'Frank', 3, 110000),
(10, 'Grace', 2, 92000),
(11, 'Heidi', 3, 95000);

INSERT INTO Dept_4 (department_id, department_name) VALUES
(1, 'HR'),
(2, 'Finance'),
(3, 'Engineering');

INSERT INTO Projects_4 (project_id, project_name, department_id) VALUES
(1, 'ProjectA', 1),
(2, 'ProjectB', 2),
(3, 'ProjectC', 3);


WITH cte AS
(
SELECT *,
RANK() OVER(PARTITION BY department_id ORDER BY salary DESC) AS rnk
FROM Emp_4
WHERE department_id IN (SELECT department_id FROM Projects_4)
)
SELECT c.department_id, d.department_name, c.emp_id, c.emp_name, c.salary
FROM cte c
JOIN dept_4 d
USING (department_id)
WHERE rnk <=3;



CREATE TABLE Transactions_1 (
    transaction_id INT PRIMARY KEY,
    customer_id INT,
    transaction_amount DECIMAL(10, 2),
    transaction_date TIMESTAMP
);

INSERT INTO Transactions_1 (transaction_id, customer_id, transaction_amount, transaction_date) VALUES
(1, 101, 100.00, '2024-01-01 10:00:00'),
(2, 101, 110.00, '2024-01-02 11:00:00'),
(3, 101, 120.00, '2024-01-03 12:00:00'),
(4, 101, 105.00, '2024-01-04 13:00:00'),
(5, 101, 130.00, '2024-01-05 14:00:00'),
(6, 102, 50.00, '2024-01-01 09:00:00'),
(7, 102, 60.00, '2024-01-02 10:00:00'),
(8, 102, 70.00, '2024-01-03 11:00:00'),
(9, 102, 75.00, '2024-01-04 12:00:00'),
(10, 102, 65.00, '2024-01-05 13:00:00'),
(11, 102, 80.00, '2024-01-06 14:00:00');


WITH SortedTransactions AS (
    SELECT 
        transaction_id,
        customer_id,
        transaction_amount,
        transaction_date,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY transaction_date) AS row_num,
        LAG(transaction_amount) OVER (PARTITION BY customer_id ORDER BY transaction_date) AS prev_transaction_amount
    FROM Transactions_1
),
Islands AS (
    SELECT 
        transaction_id,
        customer_id,
        transaction_amount,
        transaction_date,
        row_num,
        CASE
            WHEN prev_transaction_amount IS NULL OR prev_transaction_amount >= transaction_amount THEN 1
            ELSE 0
        END AS is_new_group
    FROM SortedTransactions
),
GroupIdentifiers AS (
    SELECT 
        transaction_id,
        customer_id,
        transaction_amount,
        transaction_date,
        row_num,
        SUM(is_new_group) OVER (PARTITION BY customer_id ORDER BY row_num) AS group_id
    FROM Islands
)

    SELECT
        customer_id,
        group_id,
        MIN(transaction_id) AS start_transaction_id,
        MAX(transaction_id) AS end_transaction_id,
        MIN(transaction_amount) AS start_amount,
        MAX(transaction_amount) AS end_amount,
        COUNT(*) AS transaction_count
    FROM GroupIdentifiers
    GROUP BY customer_id, group_id;
    
CREATE TABLE Emp_5 (
    emp_id INT PRIMARY KEY,
    emp_name VARCHAR(100),
    department VARCHAR(100)
);

CREATE TABLE Sales_5 (
    sale_id INT PRIMARY KEY,
    emp_id INT,
    sale_amount DECIMAL(10, 2),
    sale_date DATE
);

INSERT INTO Emp_5 (emp_id, emp_name, department) VALUES
(101, 'John', 'Sales'),
(102, 'Jane', 'Sales'),
(103, 'Alice', 'Marketing'),
(104, 'Bob', 'Marketing'),
(105, 'Charlie', 'HR');

INSERT INTO Sales_5 (sale_id, emp_id, sale_amount, sale_date) VALUES
(1, 101, 2000.00, '2023-01-01'),
(2, 101, 3000.00, '2023-02-01'),
(3, 102, 2500.00, '2023-01-15'),
(4, 102, 2500.00, '2023-02-15'),
(5, 103, 1500.00, '2023-01-05'),
(6, 103, 1500.00, '2023-02-05'),
(7, 104, 2000.00, '2023-01-20'),
(8, 104, 500.00, '2023-02-20'),
(9, 105, 1000.00, '2023-03-01');

WITH TotalSales AS
(
SELECT e.department, e.emp_id, e.emp_name, SUM(sale_amount) AS total_sales
FROM Sales_5
JOIN Emp_5 e
USING (emp_id)
GROUP BY e.emp_id, emp_name
),
Sales_rank AS 
(
SELECT department, emp_id, emp_name, total_sales,
DENSE_RANK() OVER(PARTITION BY department ORDER BY total_sales DESC) r
FROM TotalSales
)
SELECT department, emp_id, emp_name, total_sales
FROM Sales_rank
WHERE r = 1;

-- ************************************************
CREATE TABLE Emp_6 (
    emp_id INT PRIMARY KEY,
    emp_name VARCHAR(100)
);

CREATE TABLE Sales_6 (
    sale_id INT PRIMARY KEY,
    emp_id INT,
    sale_amount DECIMAL(10, 2),
    sale_date DATE,
    FOREIGN KEY (emp_id) REFERENCES Emp_6(emp_id)
);

INSERT INTO Emp_6 (emp_id, emp_name) VALUES
(101, 'John'),
(102, 'Jane');

INSERT INTO Sales_6 (sale_id, emp_id, sale_amount, sale_date) VALUES
(1, 101, 2000.00, '2023-01-01'),
(2, 101, 3000.00, '2023-02-01'),
(3, 101, 3000.00, '2023-03-01'),
(4, 102, 2500.00, '2023-01-15'),
(5, 102, 2500.00, '2023-02-15'),
(6, 102, 2500.00, '2023-03-15');

SELECT emp_id, e.emp_name, DATE_FORMAT(sale_date, '%Y-%m-01') AS month, 
ROUND(AVG(sale_amount) OVER(PARTITION BY emp_id ORDER BY sale_date ROWS BETWEEN 2  PRECEDING AND CURRENT ROW),2) moving_avg_sales
FROM Sales_6 s
JOIN Emp_6 e USING (emp_id);


-- ******************************************************** 

CREATE TABLE Products_7 (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(100)
);

INSERT INTO Products_7 (product_id, product_name, category) VALUES
(1, 'Product A', 'Category 1'),
(2, 'Product B', 'Category 1'),
(3, 'Product C', 'Category 2'),
(4, 'Product D', 'Category 2');


CREATE TABLE Sales_7 (
    sale_id INT PRIMARY KEY,
    product_id INT,
    sale_amount DECIMAL(10, 2),
    sale_date DATE,
    FOREIGN KEY (product_id) REFERENCES Products_7(product_id)
);

INSERT INTO Sales_7 (sale_id, product_id, sale_amount, sale_date) VALUES
(1, 1, 100.00, '2023-01-01'),
(2, 1, 150.00, '2023-01-15'),
(3, 2, 200.00, '2023-01-20'),
(4, 3, 300.00, '2023-01-10'),
(5, 4, 250.00, '2023-02-01'),
(6, 3, 100.00, '2023-02-15'),
(7, 2, 150.00, '2023-02-20'),
(8, 1, 300.00, '2023-03-01');


EXPLAIN WITH cte AS
(
SELECT p.category, p.product_name, DATE_FORMAT(sale_date, '%Y-%m-01') AS month, product_id, 
ROUND(AVG(sale_amount),2) AS avg_sales
FROM Sales_7 s
JOIN Products_7 p USING (product_id)
GROUP BY product_id, month
),
ranked AS(
SELECT *, 
DENSE_RANK() OVER(PARTITION BY category, month ORDER BY avg_sales DESC, month ASC) rnk
FROM cte)
SELECT  category, month, product_name, avg_sales FROM ranked
WHERE rnk = 1;

-- *************

EXPLAIN WITH AvgSales AS (
    SELECT 
        p.category, 
        p.product_name, 
        DATE_FORMAT(s.sale_date, '%Y-%m-01') AS month, 
        ROUND(AVG(s.sale_amount), 2) AS avg_sales
    FROM Sales_7 s
    JOIN Products_7 p USING (product_id)
    GROUP BY p.category, p.product_name, DATE_FORMAT(s.sale_date, '%Y-%m-01')
),
RankedSales AS (
    SELECT 
        category, 
        product_name, 
        month, 
        avg_sales,
        DENSE_RANK() OVER(PARTITION BY category, month ORDER BY avg_sales DESC) AS rnk
    FROM AvgSales
)
SELECT 
    category, 
    month, 
    product_name, 
    avg_sales
FROM RankedSales
WHERE rnk = 1;

-- ********************************
CREATE TABLE Products_8 (
    product_id INT PRIMARY KEY,
    name VARCHAR(100),
    details JSON
);

INSERT INTO Products_8 (product_id, name, details) VALUES
(1, 'Laptop', '{"specs": [{"type": "processor", "value": "Intel i7"}, {"type": "ram", "value": "16GB"}, {"type": "storage", "value": "512GB SSD"}], "prices": [1200, 1300, 1250], "in_stock": true}'),
(2, 'Smartphone', '{"specs": [{"type": "processor", "value": "Snapdragon 888"}, {"type": "ram", "value": "8GB"}, {"type": "storage", "value": "128GB"}], "prices": [800, 850, 825], "in_stock": false}');


SELECT * FROM Products_8;

SELECT product_id, name,
JSON_UNQUOTE(JSON_EXTRACT(details, '$.specs[0].value')) AS processor,
JSON_UNQUOTE(JSON_EXTRACT(details, '$.specs[1].value')) AS ram,
JSON_UNQUOTE(JSON_EXTRACT(details, '$.specs[2].value')) AS storage,
JSON_UNQUOTE(JSON_EXTRACT(details, '$.prices[0]')) AS price_1,
JSON_UNQUOTE(JSON_EXTRACT(details, '$.prices[1]')) AS price_2,
JSON_UNQUOTE(JSON_EXTRACT(details, '$.prices[2]')) AS price_3,
JSON_UNQUOTE(JSON_EXTRACT(details, '$.in_stock')) AS in_stock
FROM Products_8;

SELECT product_id, name,
JSON_UNQUOTE(JSON_EXTRACT(details, '$.specs[0].value')) AS processor,
JSON_UNQUOTE(JSON_EXTRACT(details, '$.specs[1].value')) AS ram,
JSON_UNQUOTE(JSON_EXTRACT(details, '$.specs[2].value')) AS storage,
ROUND((CAST(JSON_UNQUOTE(JSON_EXTRACT(details, '$.prices[0]')) AS DECIMAL(10, 2)) + 
CAST(JSON_UNQUOTE(JSON_EXTRACT(details, '$.prices[1]')) AS DECIMAL(10, 2)) +
CAST(JSON_UNQUOTE(JSON_EXTRACT(details, '$.prices[2]')) AS DECIMAL(10, 2))) / 3, 2) AS avg_prices,
JSON_UNQUOTE(JSON_EXTRACT(details, '$.in_stock')) AS in_stock
FROM Products_8;











