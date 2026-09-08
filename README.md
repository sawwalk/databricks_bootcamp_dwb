# databricks_bootcamp_dwb
A bootcamp created by Data With Baraa

## Notes:
local file path: Documents/'Data Analyst+Engineer'/projects/databricks_data_lakehouse_project/databricks_bootcamp_dwb

Is it standard practice to create a timestamp column showing when data was processed in the silver table in a medalion architecture?

## prompts
### For silver_crm_cust_info
---
I want to now clean up duplicates from dup_rows_cst_key in df.
the rows in dup_rows_cst_key are the candidates that can be dropped.

the output df should contain exactly one row for each distinct cst_key. 

Keep the most complete row for each cst_key amoung the candidates and drop the others.  

---
Result: Bad


Retry (I went back and added a temporary index to df to make identifying the rows to drop easier) I then chained multiple prompts together as follows:
---

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

---
Result: good

---
For normalizing strings:

run agains df_null_clean. 
- In cst_marital_status map S to single and M to married
- In cst_gndr map F to female and M to male

---
Result: good

### silver_crm_prd_info
## Phase3 - Building Silver Layer


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

---
Result: medium

Feedback:
---
There was no initial inspection of the data so I added that in. This is not important for the pipeline but an initial look is improtant for the reader.

You should always inspect rows with duplicate prd_keys during exploration. After doing this I found that the duplicates showed price changes over time. Since these duplicates hold information it is not appropriate to drop them in the silver layer as was done initially. I do not think we should be dropping any rows due to duplication in this notebook. 

The way you handled null values and the null costs was good, flag it as something to cross check with other tables once the silver layer is done (not now)

There is an inconsistency with the prd_end_dt being smaller than prd_start_dt, however the nulls in prd_end_dt make sense because that means that products are ongiong. Please explore this and try to gain an understanding about what went wrong.  Give me multiple options on how to proceed, do not do anything until I have reviewed the plan. 

I believe that product_cost should be enforced as an float data type since currency generally has decimals and this column should be able to support aggrigations like mean and variance. 

T = Touring in prd_line, flag these normalizations as something to cross check with databricks_bootcamp_dwb.bronze.erp_px_cat_g1v2 once the silver layer is done (not now)

Everything else looks good at the moment. 

Please read over my feedback and adjust the notebook accordingly. 


I have decided that I will handle the date issue by treating all of the rows with null end_date as correct but switching the start and end dates for all other rows.
Of course I understand that this approach contains risk and I want it to be documented as such, please add this to the flagged items. 


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

### For splitting prd_key
---
There is another transformation that I forgot to add in silver_crm_prd_info. 

CONTEXT:
the column prd_key is a combined column that contains both the product key and product category id. The category id is located in the first 5 characters of the string, a '-' then separates the category id from the product key so the product key goes from the 7th character to the end. 

TASK:
Add a transformation that separates the category id and the product key into two separate columns named category_id and product_key respectively.

### silver_multi-table_EDA
ROLE: You are a data engineer who is creating a medalion architecture pipline with over 10 years of experience.

CONTEXT: This silver_multi-table_EDA notebook is supposed to provide a place to conduct EDA and perform data quality checks involving multiple tables in the silver layer -- databricks_bootcamp_dwb.silver.

PHILOSOPHY: The intended workflow is to create an initial draft of all the notebooks that handle the silver transformations for each table. Then verify in this notebook by checking consistancy accross tables and then iterating on the silver notebooks if issues arise.

TASK:
Start building the silver_multi-table_EDA.
- Introduce it's purpose in a markdown at the top of the notebook.
- Preview the first 10 rows of every table in the silver schema.
- Identify primary and forign keys in each table.
- Identify compatible tables based on their primary and forign keys and test the joins between these tables.

CONSTRAINTS:
Do not transform the data in any way. This notebook is exploration only. 
Only use sql to query the data.


Feedback:
This is a good first draft, Here are some things to refine.
At the very start of the notebook I want you to go through each table and identify all of the columns that contain id, num or key and display them as potential join columns. You can use this format bellow to reccord the potential join columns and  relationships between the tables. Create one at the start of the notebook to represent the best 1st guess. 

Then after the 1st wave of testing make a second version of the join coumns, keeping the confirmed relationships and trying new ones.

Example diagram:
### Join columns
crm_customers:
- customer_id
- customer_key --> erp_customers.customer_id

crm_products:
- product_id
- product_key
- category_id --> erp_products.product_id

crm_sales:
- order_number
- product_key
- customer_id

erp_customer_location:
- customer_id --> crm_customers.customer_key, crm_sales.customer_key

erp_customers:
- customer_id --> crm_customers.customer_key, crm_sales.customer_key

erp_products:
- product_id --> crm_products.category_id

During testing:
When matches are not found during the testing phase provide 3 samplese of the tested columns from each table.

Tests to try:
test erp_customers.customer_id and erp_customer_location.customer_id  with crm_customers.customer_key
test crm_products.category_id with erp_products.product_id

Did not test joins between erm tables. 
Did not test joins between erm tables and crm_sales

Feedback 2:
I need a clearer summary from the join relationship diagram.
If partial matches are found between columns then instead of writing (TEST RESULTS ABOVE) write a tick and then include the percentage of orphaned records in brackets () with respect to the left hand table. 

customer_key → erp_customers.customer_id (TEST RESULTS ABOVE)

in this example customer_key is the left hand table.

Feedback 3:
I believe that crm_customers.customer_id are substrings of erp_customer_location.customer_id and erp_customers.customer_id
please test this theory.

Test if crm_products.category_id matches erp_products.product_id once both columns are normalized so that all separators are - and not _. 

Document findings. 

Feedback 3:
I thought of something that I should have done at the very begging. and I want to implement it now so that I can have a reccord of it for next time. 

After identifying all of the potential join columns display at least the first 100 rows of each side by side, try to group them together based on the initial hypothesized join relationship table.

---

## Prompt to Claude
### Prompt 1
ROLE: I want you to help me with prompt engineering and workflow design in the context of data cleaning as a data engineer.

CONTEXT: I am working in databricks creating a pipeline using the medalion architecture.

TASK 1: Please help me to create a refined prompt for this join diagram to document relationships between tables. The idea is to create a hypothesis join diagram at the start of the analysis that contains initial guesses of which columns in each table relate to each other, then test the joins in the hypothesis and create a second join diagram with the results.


HYPOTHESIS JOIN DIAGRAM EXAMPLE: 

**crm_customers:**
* customer_id (primary key) → crm_sales.customer_id ?
* customer_id (primary key) → erp_customer_location.customer_id ? 
  * customer_id is substring of erp_customer_location.customer_id suspected
* customer_key → erp_customers.customer_id ?

**crm_products:**
* product_id (primary key) → crm_sales.product_key
* product_key
* category_id → erp_products.product_id ? 
  * erp_products.product_id contains whitespace suspected
  * category_id is substring of erp_products.product_id suspected

**crm_sales:**
* order_number
* product_key
* customer_id → erp_customer_location.customer_id ?
* customer_id → erp_customers.customer_id ?

**erp_customer_location:**
* customer_id (primary key) → erp_customers.customer_id ?

**erp_customers:**
* customer_id (primary key)

**erp_products:**
* product_id (primary key)

Each table is listed in bold. Each potential join column belonging to each table is listed under it's bold heading. Relationships between a tables column and another tables column are indicated with an arrow. Indented dot points detail suspected data quality issues that will need to be resolved for the joins to work. Note each relationship works two ways but they are only documented once in the diagram. The user fills the diagram from the top down so that for each table the user only considers potential relationships from tables bellow as the relationships from the tables above should already be reccorded, thus we don't have to write relationships twice. 

The hypothesis diagram is supposed to be a colaborative effort between AI and human. The AI will be called appon to make an initial draft according to it's reasoning and then the human will inspect it and make edits and add suspected data quality issues. The point of the diagram is to find an effective way for the human to organize their thoughts and to communicate them with the AI. If this diagram is hard for the AI to read and interpret then it needs to be re-designed. 


RESULT JOIN DIAGRAM EXAMPLE:
**crm_customers:**
* customer_id (primary key) → crm_sales.customer_id ✓ **(59.7% orphaned)**
* customer_id (primary key) → erp_customer_location.customer_id ✓ (0% orphaned)
  * customer_id is substring of erp_customer_location.customer_id confirmed ✓
* customer_key → erp_customers.customer_id ❌ (100% orphaned)

**crm_products:**
* product_id (primary key) → crm_sales.product_key ✓ (0% orphaned)
* product_key
* category_id → erp_products.product_id ❌ (100% orphaned)
  * erp_products.product_id contains whitespace suspected ❌ (whitespace was not present)
  * category_id is substring of erp_products.product_id suspected ❌ (substring matching failed)

**crm_sales:**
* order_number
* product_key
* customer_id → erp_customer_location.customer_id  ✓ (0% orphaned)
* customer_id → erp_customers.customer_id  ✓ (0% orphaned)

**erp_customer_location:**
* customer_id (primary key) → erp_customers.customer_id  ✓ (0% orphaned)  

**erp_customers:**
* customer_id (primary key)

**erp_products:**
* product_id (primary key)


TRANSFORMATION TABLE: 
AI creates a table that documents all of the transformations that were used to handle data quality issues in columns
|table|column|transformation|

As you can see the result join diagram shows the quality of each join as well as documenting outcomes with date quality issues. A summary of the data transformations used appears directly bellow the result diagram.

Please evaluate my prompts and look for any holes or inconsistencies, please quiz me rigorously until you have enough information to make a A+ prompt that is clear, efficient and effective.

### Prompt 2
Now that the join diagram is sorted out I will give the context of where to use it.

I want to make a pre-silver_multi-table_EDA notebook primarily for identifying relationships between tables.

Here is the initial workflow
* AI uses pyspark to load each bronze table into a dataframe and goes through each table and identifies all of the columns that contain id, num or key and display them as potential join columns.
* The user then double checks to see if the AI has missed anything (they can browse columns names in unity catalogue or request samples of each table if column names are not clear)
* Once potential join columns are agreed on AI displays the first 100 rows of each of them side by side with an attempt to group them e.g. customer related vs product related.
* Then AI creates the first draft of the hypothesis join diagram, which the user can modify based on what they see in the column samples in the previous step.
* The AI reads the final hypothesis join diagram and asks the user for instructions if any data quality issues are identified.
* The AI then transforms each dataframe appropriately and tests the joins between each table.
* The AI automotically generates the result table that documents findings. 
* If there are still questions then the user may go back and add more test the hypothesis diagram.

This notebook is purely used for exploration, we use pyspark dataframes to make temporary copies of the data that we can manipulate separately.

## table_relationship_exploration Prompts
ROLE: You are a data engineer assisting me in mapping join relationships across
tables in a medallion architecture pipeline.

CONTEXT: This is an exploration
of tables in the bronze layer to identify transformations to carry out in the
silver layer. Include all tables from databricks_bootcamp_dwb.bronze.

TASK: Work through the following steps to produce a HYPOTHESIS JOIN DIAGRAM,
using only schema-level metadata — column names, data types, table/column
comments, and any join patterns visible in existing queries or notebooks.
Do NOT run any queries or inspect row-level data.

STEP 1 — IMPORT & LOCK ORDER:
Import each table into a dataframe, in the order the tables appear in the
schema (as returned by your catalog/table listing). This order is now fixed
for the rest of the analysis — do not reorder tables at any later step.

STEP 2 — IDENTIFY CANDIDATE JOIN COLUMNS:
For each table, identify columns that could plausibly serve as join keys —
primary keys, foreign-key-like columns, and any column matching an ID/key/
code naming pattern.

STEP 3 — DISPLAY FIRST 100 ROWS OF CANDIDATE COLUMNS GROUPED BY SEMANTIC FIELD:
Display the first 100 rows of all candidate join columns grouped by semantic field (e.g. every
column that looks like a "customer identifier" together, every column that
looks like a "product identifier" next to eachother), independent of table order.
- Columns from the same table that belong to the same semantic field stay
  adjacent within that group.
- Every column must still show which table it came from.
- Format per group, using `table.column.dtype` so table, column, and type are
  all visible in the column name table_a.column_x.dtype
 
  (columns from the same table that share a semantic field stay adjacent,
  as above; table order within a group otherwise follows the locked order
  from Step 1)

STEP 4 — BUILD THE HYPOTHESIS JOIN DIAGRAM:
Using the fixed order from Step 1, work top-down. For each table, only
propose relationships to tables that appear BELOW it in that order — tables
above have already had their outgoing relationships documented, so don't
repeat a relationship from the other direction.

FOR EACH TABLE:
- Bold table name as a heading.
- List every column that could plausibly join to a table below it, as:
  `column_name (primary key, if applicable) → target_table.target_column ?`
- Base each hypothesis on: (a) matching/similar column names, (b) semantic
  meaning (e.g. "customer_id" vs "cust_key"), (c) data type compatibility,
  (d) join patterns found in existing notebooks/queries.
- If you suspect a data-quality mismatch that would block the join (substring
  match, whitespace, casing, type mismatch, etc.), add an indented sub-bullet:
  `* [issue] suspected`
- List a column with no hypothesis on its own line, with no arrow.
- No prose inside the diagram. Put reasoning/notes below it, separately.
- Record uncertain relationships with "?" rather than omitting them — they
  get validated at the test stage, not filtered out now.

OUTPUT:
1. The candidate-columns-by-semantic-field table(s) from Step 3.
2. The hypothesis join diagram from Step 4, formatted exactly as above.
3. A short "assumptions / things to double-check" note underneath, if any.

Prompt 2:

ROLE: You are a data engineer helping me validate join hypotheses and
document the results in a medallion pipeline.

CONTEXT: Below is our hypothesis join diagram, which I've reviewed and
annotated with my own insight and suspected transformations.

### HYPOTHESIS JOIN DIAGRAM

**crm_cust_info:**
* cst_id (primary key) → crm_sales_details.sls_cust_id ?
* cst_key → erp_cust_az12.CID ?
  * substring/prefix match suspected (CID = "NAS" + cst_key)
* cst_key → erp_loc_a101.CID ?
  * hyphen separator suspected (CID = "AW-00011028" vs cst_key = "AW00011028")

**crm_prd_info:**
* prd_id (primary key)
* prd_key → crm_sales_details.sls_prd_key ?
  * substring/prefix match suspected (prd_key = "CO-RF-FR-R92R-62" vs sls_prd_key = "BK-R93R-44")
* prd_key → erp_px_cat_g1v2.ID ?
  * substring/suffix and underscore separator suspected (prd_key = "CO-RF-FR-R92R-62" vs ID = "CO_FO")
* prd_nm

**crm_sales_details:**
* sls_ord_num (primary key)
* sls_prd_key
* sls_cust_id

**erp_cust_az12:**
* CID → erp_loc_a101.CID ?
  * format mismatch suspected (erp_cust_az12.CID = "NASAW00011028" vs erp_loc_a101.CID = "AW-00011028")

**erp_loc_a101:**
* CID

**erp_px_cat_g1v2:**
* ID

STEP 1 — CLARIFY (do this first, before running any new code to test joins):
Review the diagram and my annotations. Ask me about anything ambiguous —
e.g. which transformation to apply for a flagged data-quality issue, or
what orphan-rate threshold counts as "acceptable" for a join to pass.
Stop and wait for my answers before moving to Step 2.

STEP 2 — TEST (after I've answered):
Write and run code to test every relationship in the diagram, against the
FULL tables — not just the 100-row sample from Prompt 1, which was only for
eyeballing candidate columns:
- Calculate % orphaned records (rows in the source column with no match in
  the target column).
- Mark ✓ if the join resolves within the threshold we agreed, ❌ if not.
- For each suspected data-quality issue, check it against real data and
  mark "confirmed ✓" or "❌ (state why it didn't hold)".

STEP 3 — RESULT DIAGRAM:
Write this directly into a markdown cell — do not generate it as code or a
printed code-cell output, same as the hypothesis diagram. Reproduce the full
diagram in the same structure and table order as the hypothesis, with:
- ✓ / ❌ next to every tested relationship.
- `(X% orphaned)` in plain (non-bold) parentheses next to each one.
- Confirmed/failed status appended to each data-quality sub-bullet.

STEP 4 — TRANSFORMATION TABLE (append immediately below the result diagram):
| table | column | transformation |
|---|---|---|
Only include a row for issues you actually resolved. If a join fails with
no clear fix, list it separately underneath as "Unresolved" rather than
guessing a transformation.

CONSTRAINTS:
- Keep formatting identical to the hypothesis diagram (bold headers,
  indented sub-bullets).
- Don't invent transformations for joins that failed outright.


Go through every notebook in /Workspace/Users/samwwalks@gmail.com/databricks_bootcamp_dwb/bike_lakehouse/silver/crm and /Workspace/Users/samwwalks@gmail.com/databricks_bootcamp_dwb/bike_lakehouse/silver/erp. For every notebook can you find documentation for a Source and Target table near the top of the notbook? 


I want to re-do the join analysis after cell 10. I want to provide more guidence.
I want you to write the code and find the most efficent methods of carying out the instructions that I am going to give you.
Let's work through the hypothesis join diagram one join at a time starting with crm_customers.cst_id (primary key) → crm_sales.sls_cust_id. 
For this case I want to just test joining the columns as they are, no transformations required. Please create a test for this. Try to keep the code succinct and readable.

Ok this looks good but lets bundle this code into a function since we will probably use it again

Great lets move onto testing the new the next relationship which is crm_customers.cst_key → erp_customers.CID however it looks like crm_customers.cst_key and erp_customers.CID may contain crm_customers.cst_id as a substring here are examples of each followed by crm_customers.cst_id AW00011028 NASAW00011028 11028. 

To start with I want to confirm that crm_customers.cst_key always contains its unique crm_customers.cst_id.
For every cst_id in crm_customers check that it can be found in the last 5 characters of cst_key

next compare the number of unique rows in crm_customers.cst_key as it is and crm_customers.cst_key when we just take the last 5 characters. Nothing fancy just print out the counts.

Now test if crm_customers.cst_key → erp_customers.CID join together under the transformation that splits each column into 2 parts. Part 1 is the prefix and part two is the last 5 characters of each column.