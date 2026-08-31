# databricks_bootcamp_dwb
A bootcamp created by Data With Baraa

## prompts
---
I want to now clean up duplicates from dup_rows_cst_key in df.
the rows in dup_rows_cst_key are the candidates that can be dropped.

the output df should contain exactly one row for each distinct cst_key. 

Keep the most complete row for each cst_key amoung the candidates and drop the others.  

Result: Bad

Retry (I went back and added a temporary index to df to make identifying the rows to drop easier) I then chained multiple prompts together as follows:

- Modify dup_rows_cst_key so that it contains a column that counts the number of nulls in each row.
Store as: dup_cst_key_nulls.
- Create a column in dup_cst_key_nulls called keep. Go through each unique cst_key and compare the rows that contain this cst_key. Among the rows with the same cst_key assign True in the keep column to the row with the smallest number of nulls and False otherwise.
- Use the tmp_index column to remove all of the rows with keep == False in dup_cst_key_nulls from df
    - This will not work, I do not want to reduce df down to the rows with keep == True I want to simply remove the rows with keep == False but still retain all of the other rows in df that do not appear in dup_cst_key_nulls.

---
For cleaning nulls:
- check how many null are in each column of df_no_dup
- display the rows in df_no_dup with null cst_id

Look at the code blocks after the nulls heading in this notebook. 

I only dropped the rows where the cst_id was null because all of the other rows appart from cst_key, temp_index where also null. 

I verified this by manually checking but that will not be good enough for a data pipeline. 

Can you re-design this workflow so that it drops all of the rows where all columns except cst_key and temp_index are null without relying on human verification.

Result: good

---
For normalizing strings:

run agains df_null_clean. 
- In cst_marital_status map S to single and M to married
- In cst_gndr map F to female and M to male

Result: good
 