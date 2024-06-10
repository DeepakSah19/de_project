# PROBLEM STATEMENT

1. Question 47
Find the item brands and categories for each store and company, the monthly sales figures for a specified year, where the monthly sales figure deviated more than 10% of the average monthly sales for the year, sorted by deviation and store. Report deviation of sales from the previous and the following monthly sales.
Qualification Substitution Parameters
YEAR.01 = 1999
SELECTONE = v1.i_category, v1.i_brand, v1.s_store_name, v1.s_company_name
SELECTTWO = ,v1.d_year, v1.d_moy

2. Question 39
This query contains multiple, related iterations:
Iteration 1: Calculate the coefficient of variation and mean of every item and warehouse of two consecutive 
months
Iteration 2: Find items that had a coefficient of variation in the first months of 1.5 or large
Qualification Substitution Parameters:
YEAR.01 = 2001
MONTH.01 = 1