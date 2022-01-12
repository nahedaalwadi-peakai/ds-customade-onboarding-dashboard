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
query <-paste0("SELECT * FROM stage.customer_start_dates;")
data <- DBI::dbGetQuery(con, query)

data$remove <- 0
new_rows <- setNames(data.frame(matrix(ncol = ncol(data), nrow = 0)), colnames(data))

for (i in 1:nrow(data)) {
  if ((str_detect(data[i,]$product, "&") | str_detect(data[i,]$product, ",")) == TRUE) {
    products <- str_split(data[i,]$product, "&", simplify = T)
    row <- data[i,]
    data[i,]$remove = 1
    for (ii in 1:ncol(products)) {
      product <- str_trim(products[1,ii])
      new <- row
      new$product <- product
      new_rows<-rbind(new_rows, new)
    }
  }}

cleaned <- data %>% filter(remove!=1)
cleaned <- rbind(cleaned, new_rows)
cleansed <- cleaned %>% mutate(product = str_trim(product)) %>% select(-remove)

# write to s3
bucket <- "kilimanjaro-prod-datalake"
path <- "customade/datascience/customer_start_dates/"
path <- paste(path, "customer_start_dates-", Sys.Date(), '.csv', sep='')

s3write_using(cleansed,
              FUN = write.csv,
              object = path,
              bucket = config$s3_bucket,
              row.names = FALSE)

DBI::dbGetQuery(con, paste0("copy transform.customer_start_dates
                  from '",  paste('s3:/', bucket, path, sep='/'), "'
                  iam_role 'arn:aws:iam::794236216820:role/RedshiftS3Access'
                  NULL as 'NA'
                  EMPTYASNULL
                  ignoreheader 1
                  csv;", sep=''))


