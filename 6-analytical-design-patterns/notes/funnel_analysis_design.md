# FUNNEL ANALYSIS

#### DE Interview that gets it right

- Care about number of table scans
    - COUNT(CASE WHEN) very powerful combo during iv and on job
    - Cumulative table design minimizes table scans
- Clean SQL code
    - Using CTE
        - Some edge cases with old db version or different db where performance might hurt
    - Using aliases in column esp for derived column
- Use EXPLAIN more
    - Abstract syntax tree- hierarchical order plan of sql

#### Advanced SQL Techniques to Try Out

- GROUPING SETS / GROUP BY CUBE / GROUP BY ROLLUP
- Self-joins
- Window Functions - LAG(), LEAD(), ROWS()
- CROSS JOIN UNNEST / LATERAL VIEW EXPLODE
    - Almost the same, cross join is more like db, where lateral view is more on the big data tools

#### GROUPING SETS

- GROUP BY GROUPING SETS(x,y) is roughly equivalent to GROUP BY x UNION ALL GROUP BY y
- UNION ALL hurts performance, one of the slowest keyword
    - Readability - dont have to copy paste multiple time

    -   ```sql
        -- Using GROUPING SETS
        SELECT 
            department,
            SUM(salary) as total_salary
        FROM employees
        GROUP BY GROUPING SETS (
            (department),  -- Group by department
            ()            -- Grand total
        );

        -- Same result using UNION ALL
        SELECT department, SUM(salary) as total_salary
        FROM employees
        GROUP BY department
        UNION ALL
        SELECT NULL, SUM(salary)
        FROM employees;
        ```
- MAKE SURE column is never null in GROUPING SETS for excluded columns
    - Use COALESCE to make it not NULL
    - Because if it is not, you will never know if the data is NULL because it is excluded, or it is really NULL

#### CUBE

- Gives you all possible permutation for grouped dimension

    -   ```sql
        -- Using CUBE
        SELECT 
            department,
            job_role,
            SUM(salary) as total_salary
        FROM employees
        GROUP BY CUBE(department, job_role);
        ```

- Results
    department | job_role | total_salary
    -----------+----------+-------------
    HR         | Manager  | 50000        -- HR Managers
    HR         | Staff    | 40000        -- HR Staff
    IT         | Manager  | 60000        -- IT Managers
    IT         | Staff    | 45000        -- IT Staff
    HR         | NULL     | 90000        -- All HR
    IT         | NULL     | 105000       -- All IT
    NULL       | Manager  | 110000       -- All Managers
    NULL       | Staff    | 85000        -- All Staff
    NULL       | NULL     | 195000       -- Grand total

- At limit 3, maybe 4 if you want more fancy
    - But definitely not 5 because it will blow up the records, imagine 5!

#### ROLLUP

- ROLLUP creates hierarchical grouping from left to right

    -   ```sql
        -- Using ROLLUP
        SELECT 
            region,
            department,
            SUM(salary) as total_salary
        FROM employees
        GROUP BY ROLLUP(region, department);
        ```
- Results

    -   region  | department | total_salary
        --------+------------+-------------
        North   | HR        | 50000        -- North HR total
        North   | IT        | 60000        -- North IT total
        North   | NULL      | 110000       -- North total
        South   | HR        | 45000        -- South HR total
        South   | IT        | 55000        -- South IT total
        South   | NULL      | 100000       -- South total
        NULL    | NULL      | 210000       -- Grand total

#### CUBE vs ROLLUP vs GROUPING SETS
- ROLLUP is better than CUBE in a drill-down report
- CUBE is mostly never used
    - Problem with cube is it will do combinations with things that you don't care
    - Will waste compute then
- GROUPING SETS for fine control and fixed sets of combinations
- CUBE may be used for EDA and you just want to explore things

#### Window Functions
- The function (RANK, SUM, AVG, DENSE_RANK)
    - DENSE_RANK better than RANK cause RANK skips value
- The window (PARTITION BY, ORDER BY, ROWS)
    - ROWS always look at current rows and backwards

#### Data Modelling vs Advanced SQL
- If your data analysts need to do SQL gymnastics to solve their analytics, problem, you're doing a bad job as DE
    - But sometimes analyst just want explore more
- If your analysts is doing the same query over and over again, consider materialize them
- Need understanding on what they present so that you know their bottleneck
- Your job is to make data analyst works faster

#### Symptoms of Bad Data Modelling

- Slow dashboard
    - Consider grouping sets for daily
    - Make sure it is aggregated, aggregate for dimension that you care about
- Queries with a weird number of CTEs (analyst side)
    - Consider staging table to materialize the cte data
    - Remember storage is cheaper than compute across the board
- Lots of CASE WHEN statements in the analytics queries
    - Your job to make them query simpler
