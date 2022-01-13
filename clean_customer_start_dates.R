library(aws.s3)
library(aws.ec2metadata)
library(aws.signature)
library(stringr)
library(tidyr)
library(RPostgreSQL) # REQUIRED for dbDriver("PostgreSQL"): https://stackoverflow.com/questions/29755483/connecting-postgresql-with-r?rq=1
library(dplyr)
library(DBI)
library(odbc)

set_credentials <- function(){
  
  message('Getting redshift credentials')
  
  DRIVER <- dbDriver("PostgreSQL")
  TENANT <- Sys.getenv("TENANT")
  USERNAME <- Sys.getenv("REDSHIFT_USERNAME")
  PASSWORD <- Sys.getenv("REDSHIFT_PASSWORD")
  HOST <- Sys.getenv("REDSHIFT_HOST")
  PORT <- Sys.getenv("REDSHIFT_PORT")
  
  message(paste0('USERNAME: ', USERNAME))
  message('Connecting to redshift')
  
  if (USERNAME == ''){
    
    message('Using ODBC')
    con <- DBI::dbConnect(odbc::odbc(), "customade-prod")
    
  } else {
    con <- DBI::dbConnect(DRIVER,
                          user = USERNAME,
                          password = PASSWORD,
                          dbname = TENANT,
                          host = HOST,
                          port = PORT)
  }
  return(con)
}

con <- set_credentials()

# get staged dataset:
query <-paste0("SELECT * FROM stage.customer_start_dates;")
data <- DBI::dbGetQuery(con, query)
# data <- data %>% filter(cus_customer_name == "Choice Trade Frames")


expand_multiple_entries <- function(col_name, data) {
  # add flag to indicate rows with multiple entries
  data$remove <- 0
  # create clean DF
  new_rows <<- setNames(data.frame(matrix(ncol = ncol(data), nrow = 0)), colnames(data))
  
  for (i in 1:nrow(data)) {

    # identify rows with multiple entries in col_name
    if ((str_detect(data[i,col_name], "&") | str_detect(data[i,col_name], ",")) == TRUE) {
      values <- str_split(data[i,col_name], "[&,]", simplify = T)
      row <- data[i,]
      data[i,]$remove = 1
      for (ii in 1:ncol(values)) {
        value <- str_trim(values[1,ii])
        new_row <- row
        new_row[[col_name]] <- value
        new_rows<-rbind(new_rows, new_row)
      }
    }}

  cleansed_df <- data %>% filter(remove!=1)
  cleansed_df <- rbind(new_rows, cleansed_df)
  final_df <- cleansed_df %>% select(-remove)
  return(final_df)
}

# expand product col
expanded_df <- expand_multiple_entries('product', data)
# expand customer account number
expanded_df <- expand_multiple_entries('cus_account_number', expanded_df)

# clean whitespace in all cols
clean_df <- expanded_df %>%
  mutate_if(is.character, str_trim)

# write to s3
bucket <- "kilimanjaro-prod-datalake"
path <- "customade/datascience/customer_start_dates/"
path <- paste(path, "customer_start_dates-", Sys.Date(), '.csv', sep='')

s3write_using(clean_df,
              FUN = write.csv,
              object = path,
              bucket = bucket,
              row.names = FALSE)

DBI::dbGetQuery(con, paste0("TRUNCATE TABLE transform.customer_start_dates;"))
DBI::dbGetQuery(con, paste0("copy transform.customer_start_dates
                  from '",  paste('s3:/', bucket, path, sep='/'), "'
                  iam_role 'arn:aws:iam::794236216820:role/RedshiftS3Access'
                  NULL as 'NA'
                  EMPTYASNULL
                  ignoreheader 1
                  csv;", sep=''))