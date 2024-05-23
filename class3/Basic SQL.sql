/* IN THIS PART, WE WILL BE WORKING WITH BASIC SQL STATEMENTS */

/* SELECT ALL COLUMNS FROM THE orders TABLE */





/* SELECT ALL COLUMNS FROM THE accounts TABLE */




/* SELECT ALL COLUMNS FROM THE region TABLE */


/* SELECT ALL COLUMNS FROM THE sales_reps TABLE */



/* SELECT ALL COLUMNS FROM THE web_events TABLE */




/* USING TOP statement
NOTE: Not all database systems support SELECT TOP statements. MySQL and Postgre support LIMIT clause
to select a minimum number of records while Oracle uses FETCH FIRST n ROWS ONLY and ROWNUM*/

/*
Write a query that displays all the data in the occurred_at, account_id, and channel 
columns of web_events table, and limits the output to only the first 10 rows.
*/





/* USING DISTINCT */

/*
Write a query to return the distinct channels in the web_events table
*/


There are 6 distinct channels on the web events table




/* USING the ORDER BY clause */

/*
Write a query to return the 10 earliest orders in the orders table. Include the id, occurred_at, and total_amt_usd.
*/


/*
Write a query to return the top 5 orders in terms of the largest total_amt_usd. Include the id, account_id, and total_amt_usd.
*/


/*
Write a query to return the lowest 20 orders interms of smallest total_amt_usd. Include the id, account-id, and total_amt_usd.
*/


/*
Write a query that displays the order_id, account_id, and total dollar amount for all the orders, sorted
first by the account_id (in ascending order), and then by the total dolar amount (in descending order).
*/


/*
Write a query that again displays the order_id, account_id, and total dollar amount for each order, but this time 
sorted first by the total dollar amount (in descending order), and then by the account_id (in ascending order).
*/



/* USING the WHERE clause */

/*
Write a query that returns the first 5 rows and all columns from the orders 
table that have a dollar amount of gloss_amt_usd greater than or equal to 1000.
*/


/*
Write a query that returns the first 10 rows and all columns from the orders table that have a total_amt_usd less than 500.
*/


/*
Filter the accounts table to include the company name, website, and the primary point 
of contact (primary_poc) just for the EOG Resources Company in the accounts table.
*/




/* USING ARITHMETIC OPERATORS */

/*
Create a column that divides the gloss_amount_usd by the gloss_quantity to find the unit price for the standard paper
paper for each order. Limit the results to the first 10 orders, and include the id and the account_id field.
*/


/*
Wrie a query that finds the percentage of revenue that comes from poster paper for each other.You will need to use only the
columns that ends with _usd. (Try to do this without using the total column). Display the id and the account_id_fields also.
*/




/* USING the LIKE operator */

/*
Write a query that returns all the companies whose name starts with 'C'.
*/



/*
Write a query that returns all companies whose names contain the string 'one' somewhere in the name.
*/



/*
Write a query that returns all companies whose names end with 's'.
*/



/* USING THE IN operator */

/*
Use the accounts table to find the account name, primary_poc, and sales_rep_id for Walmart, Target, and Nordstrom.
*/


/*
Use web-events table to find all information regarding all individuals who were contacted via channel of organic or adwords
*/




/* USING NOT operator */

/*
Use web-events table to find all information regarding all individuals who were
contacted via any method except using organic or adwords methods.
*/


/* Use the accounts table to find:
i. all the companies whose name do not start with 'c'. */


/* ii. all the companies whose names do not contain the string 'one'. */




/* USING AND and BETWEEN operators */

/*
Write a query that returns all the orders where the standard_qty is over 1000, the poster_qty is 0, and the gloss_qty is 0.
*/



/*
Using the accounts table, find all the companies whose names do not start with 'c' and end with 's'.
*/



/*
Write a query that displays the order date and gloss_qty data for all orders where gloss_qty data is between 24 and 29.
*/


/*
Use the web_events table to find all the information regarding all individuals who were contacted via the organic
or adwords channels, and started their account at any point in 2016, sorted from newest to oldest.
*/



/* USING the OR operator */

/*
Find the list of order_ids where either gloss_qty or poster_qty is greater than 4000. 
Only include the id field in the resulting table.
*/



/*
Write a query that returns a list of orders where the standard_qty is zero and either the gloss_qty or poster_qty is over 1000.
*/



/*
Find all company names that start with a 'C' or 'W', and the primary contact contains 'ana, or 'Ana', but it doesn't contain 'eana'.
*/



