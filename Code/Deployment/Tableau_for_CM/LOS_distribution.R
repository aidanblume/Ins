# # LOS Distribution

# AU: njb



# ## Setup

library(sparklyr)
config <- spark_config()
config$spark.driver.host <- Sys.getenv("CDSW_IP_ADDRESS")
spark <- spark_connect(
  master = "local",
  app_name = "read",
  config = config
)



# ## Access the HDFS table

library(dplyr)

# See what databases are outside the metastore.

src_databases(spark)

# Load table

admissions <- tbl(spark, dbplyr::in_schema("nathalie", "njb_tableau_export"))

class(admissions)

# ## Viewing some data and examining the schema

# Print the `tbl_spark` to see the names and types 
# of the columns and the first 10 rows of data, 
# for as many columns as fit on the screen:

admissions
print(admissions)
admissions %>% print()

# To print the first *`x`* rows for some other value 
# of *`x`*, specify a value for the argument `n` to the
# `print()` function:

admissions %>% print(n = 5)
admissions %>% print(n = 5, width = Inf)

# To get a vector of the column names, use `colnames()`:

colnames(admissions)
admissions %>% colnames()



# ## Counting the number of rows and columns

# To get the number of rows and columns, use 
# `sdf_nrow()` and `sdf_ncol()`. "sdf" stands for 
# "Spark DataFrame".

admissions %>% sdf_nrow()
admissions %>% sdf_ncol()
admissions %>% sdf_dim()



# ## Inspecting a column (variable)

# To select one or more columns, use `select()`:

admissions %>% select(source_facility_id)
admissions %>% select(source_facility_id, location_facility)


# To select the distinct values of one or more columns, 
# use `distinct()`:

admissions %>% distinct(member_id)
admissions %>% distinct(member_id, source_facility)


# You can also use `sdf_nrow()`, `sdf_ncol()`, or 
# `sdf_dim()` after operations like these:

admissions %>% 
  distinct(admit_date, discharge_date) %>% 
  sdf_dim()



# ### `mutate()` creates one or more new columns

admissions %>% mutate(full_name = paste(first_name, last_name))

# Use the `mutate()` verb with the `substr()` function
# to get the birth year of riders.

# You can use `mutate()` to replace existing columns, but
# note that the order of the columns may not be preserved:

# You can use `mutate()` to change the data types of 
# one or more columns:

admissions %>% mutate(
  admit_date = as.date(admit_date),
  discharge_date = as.date(discharge_date),
)

# Note that the `birth_date` column still appears in the
# printed R `tbl_spark` to be a string column, but 
# internally Spark now recognizes it as a date column.

admissions %>% mutate(
  sex = ifelse(is.na(sex), "other/unknown", sex)
)

admissions <- admissions %>% mutate(
  los = datediff(discharge_date, admit_date)
)

admissions %>% select(admit_date, discharge_date, los)



# ### `summarise()` applies aggregation functions to the data

admissions %>% summarise(
  n = n()
)

# Tip: `tally()` is a shortcut for `summarise(n = n())`:

admissions %>% tally()

admissions %>% summarise(
  num_unique_location_facility = n_distinct(location_facility)
)

admissions %>%
  group_by(ds_visit_type_id) %>%
  summarize(
    mean_los = mean(los),
    min_los = min(los),
    max_los = max(los)
  )

# histogram / graphics

# Create the summary table before you plot. 
# We use the `n` operator to count the number of times each waiting time appears.
# Some R-specific type conversion functions do not work
# with `mutate()` on `tbl_spark` objects, but they do work
# after you `collect()` the `tbl_spark` to a `tbl_df`:

frequency_distribution <- admissions %>% 
  group_by(ds_visit_type_id, los) %>% 
  summarize(n = n()) %>% 
  arrange(ds_visit_type_id, los) %>% collect()

install.packages("ggplot2")
library(ggplot2)

ggplot(frequency_distribution, aes(x=los, y=n, color=factor(ds_visit_type_id)))+   
  geom_line()+
  geom_point()

# Looks terrible because ER and Inpatient time scales are so different.

# for ER visits
# compute the +2 day cutoff and graph distribution + cutoff

er_los <- frequency_distribution %>% filter(ds_visit_type_id == 72)

er_stats <- admissions %>% 
  select(ds_visit_type_id, los) %>% 
  filter(ds_visit_type_id == 72) %>% 
  summarize(n = n(), 
            mean = mean(los),
            stdev = sd(los),
            cutoff = 2
           ) %>% 
  collect()

ggplot(er_los, aes(x=los, y=n))+   
  geom_line() +
  geom_vline(xintercept = er_stats$cutoff, colour="red") +
  geom_text(aes(x=er_stats$cutoff, label="\nCutoff at 2 days", y=100000), colour="red", angle=90, text=element_text(size=11))


er_exclude <- admissions %>% filter(ds_visit_type_id == 72) %>% filter(los > 2)

# for Inpatient visits
# compute the +3 standard deviations cutoff and graph the entire distribution with the 3sd cutoff line in red

inp_los <- frequency_distribution %>% filter(ds_visit_type_id == 70)

inp_stats <- admissions %>% 
  select(ds_visit_type_id, los) %>% 
  filter(ds_visit_type_id == 70) %>% 
  summarize(n = n(), 
            mean = mean(los),
            stdev = sd(los),
            cutoff = mean(los) +  3 * sd(los)
           ) %>% 
  collect()

ggplot(inp_los, aes(x=los, y=n))+   
  geom_line() +
  geom_vline(xintercept = inp_stats$cutoff, colour="red") +
  geom_text(aes(x=inp_stats$cutoff, label="\nCutoff at 3 standard deviations", y=10000), colour="red", angle=90, text=element_text(size=11))

inp_stats$cutoff

# A member may have an unusually log hospital stay. To determine whether the patient requires care management, call
# the facility to verify that the patient has not been discharged (data error). 

inp_call_to_verify_data <- admissions %>% filter(ds_visit_type_id == 70) %>% filter(los > inp_stats$cutoff)

# For ER and Inp separately, excludes scores that are 3 standard deviations above the mean (excludes 0.0003 or 0.03%)



# Stop the `SparkSession`:

spark_disconnect(spark)

