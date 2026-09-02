# databricks_bootcamp_dwb
A bootcamp created by Data With Baraa

## Notes:
local file path: /c/Users/samww/Documents/Data Analyst+Engineer/projects/databricks_data_lakehouse_project/databricks_bootcamp_dwb

Is it standard practice to create a timestamp column showing when data was processed in the silver table in a medalion architecture?

## prompts
### For silver_crm_cust_info
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

### silver_crm_prd_info
## Phase3 - Building Silver Layer
---

Build a new notebook that analyzes and cleans the data in databricks_bootcamp_dwb.bronze.crm_prd_info. We are building a silver layer from a bornze layer in a medalion achitecture.

**Goal**: Clean and transform our bronze data and get it ready to load into the silver layer.

Use these steps as a guide:
- [x]  Analyze data quality using SQL and List all identified issues
    - [x]  Find duplicates
    - [x]  Validate string values: Check extra spaces, Identify abbreviations to normalize
    - [x]  Validate dates values: Check Data Type, check the format, handle missing values
    - [x]  Validate numeric values
    - [x]  Standardize business key IDs to ensure tables can be joined correctly.
    - [x]  Check the name of columns and table and make a plan how to rename them to something friendly.
- [x]  Section 1: Read data Bronze Table and Load it into a DataFrame
- [x]  Section 2: Transform data
    - Fix issues one by one
    - Keep transformations small and clear
    - Avoid one large transformation block
    - Use Spark SQL or PySpark (Python)
    - Before going to next transformation always check the result “df.display()”
- [x]  Sanity checks the final DataFrame before writing
- [x]  Finalize notebook
    - [x]  Run the full notebook end to end
    - [x]  Review structure and readability
    - [x]  Add comments and documentation

The output should be a fully formatted notebook.

Result: medium

Feedback:
There was no initial inspection of the data so I added that in. This is not important for the pipeline but an initial look is improtant for the reader.

You should always inspect rows with duplicate prd_keys during exploration. After doing this I found that the duplicates showed price changes over time. Since these duplicates hold information it is not appropriate to drop them in the silver layer as was done initially. I do not think we should be dropping any rows due to duplication in this notebook. 

The way you handled null values and the null costs was good, flag it as something to cross check with other tables once the silver layer is done (not now)

There is an inconsistency with the prd_end_dt being smaller than prd_start_dt, however the nulls in prd_end_dt make sense because that means that products are ongiong. Please explore this and try to gain an understanding about what went wrong.  Give me multiple options on how to proceed, do not do anything until I have reviewed the plan. 

I believe that product_cost should be enforced as an float data type since currency generally has decimals and this column should be able to support aggrigations like mean and variance. 

T = Touring in prd_line, flag these normalizations as something to cross check with databricks_bootcamp_dwb.bronze.erp_px_cat_g1v2 once the silver layer is done (not now)

Everything else looks good at the moment. 

Please read over my feedback and adjust the notebook accordingly. 

-- 
I have decided that I will handle the date issue by treating all of the rows with null end_date as correct but switching the start and end dates for all other rows.
Of course I understand that this approach contains risk and I want it to be documented as such, please add this to the flagged items. 
---

### For silver_crm_sales_details
Create a silver layer for databricks_bootcamp_dwb.bronze.crm_sales_details in this notebook

Result: medium (it drew context from my other notbooks)

Feedback:
duplicate exploration was handled well but did not check show many nulls were in each column of the dataset (you should always do this).

You can explore the bronze data using SQL but use PySpark to upload the bronze table into a dataframe and perform opperations on it, break up the transformation step into its separate transformations and preview the data at the end of every step using display(), look at silver_crm_prd_info for an example of this. 

0 dates should be converted to nulls not imputed or calculated

Do not create any calculated fields in the silver table, those are for the gold layer.


I can see that sales_amount and sls_price are sls_sales are essentially duplicates of eachother with some null rows in each,
The nulls only appear in rows with quantity > 1. My hypothesis is that these columns should not be equal when quantity > 1 and that either sls_price should be calculated by dividing sls_sales by quantity or that sls_sales should be calculated by multiplying sls_price by quantity. 

I want to verify the sls_price column by comparing it to the prd_cost column in databricks_bootcamp_dwb.bronze.crm_prd_info. (Join the tables using left join to preserve all of the rows in crm_prd_info since it will be a many to one relationship)

Refined prompt:
CONTEXT: In this dataset, sls_sales and sls_price appear to be near-duplicates of each other, 
with nulls appearing in each column, but only in rows where quantity > 1.

HYPOTHESIS: sls_sales and sls_price should NOT be equal when quantity > 1. Instead, one of the 
following relationships should hold:
  - sls_price = sls_sales / quantity, OR
  - sls_sales = sls_price * quantity

VERIFICATION TASK:
Check the hypothesis by comparing sls_price against prd_cost in 
databricks_bootcamp_dwb.bronze.crm_prd_info.

PROCEDURE: dedupe_and_join
  Applies to an input table called `sales_input`.

  STEP 1: Reduce `sales_input` to one row per prd_key.
    - Rule: keep the first instance per prd_key.
    - Ordering: no particular order.
  → Store as: sales_deduped

  STEP 2: LEFT JOIN crm_prd_info (left/preserving table) to sales_deduped, on prd_key.
    - Relationship: many crm_prd_info rows (product versions) to one sales_deduped row per prd_key.
    - Do not drop or duplicate any crm_prd_info rows.
  → Store as: joined_result

  STEP 3: Display the full joined_result table.

---

RUN 1: Call dedupe_and_join with sales_input = crm_sales_details (all rows).

RUN 2: Call dedupe_and_join with sales_input = crm_sales_details filtered to quantity > 1.

---
After failed joins between keys:
---

NEW HYPOTHESIS: crm_sales_details contains abreviated product keys which is why the joins between crm_sales_details and crm_prd_info are not working.

KEY TABLES AND COLUMNS:
table 1:
databricks_bootcamp_dwb.bronze.crm_sales_details
table columns:
sls_prd_key 
sls_quantity
sls_price

table 2:
databricks_bootcamp_dwb.bronze.crm_prd_info
table columns:
prd_key
prd_cost

GOAL: To clean up the notebook and successfully find product key matches between crm_sales_details and crm_prd_info. 

Step 1: Undo the failed most recent analysis that attempted to compare product prices but found no matches between keys in crm_prd_info and crm_sales_details so that the notebook does not become too cluttered.

Step 2: Analyse keys in prd_key and sls_prd_key, search for patterns. In particular check if any keys in sls_prd_key are a subset of keys in prd_key.

---
After substring matching was found to be possible:
---

Now please use this substring matching technique to retest this hypothesis.

HYPOTHESIS: sls_sales and sls_price should NOT be equal when quantity > 1. Instead, one of the 
following relationships should hold:
  - sls_price = sls_sales / quantity, OR
  - sls_sales = sls_price * quantity

Do this by comparing values in sls_price with values in prd_cost and searching for matches. Search for any patterns and descrpancies. 

please display example rows from the joined tables so that I can see for myself.

---

Please re-structure the notebook so that this sales price to product cost comparison gets put into an appendix section at the end of silver_crm_sales_details. 