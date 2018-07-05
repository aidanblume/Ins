
setwd("Analysis_SWAT/Code/Plots/")

packages <- c("data.table",
              "stats",
              "ggplot2",
              "plyr",
              "dplyr",
              "reshape2",
              "tidyr",
              "sparklyr",
              "clust",
              "fpc",
              "readr",
              "caret",
              "pvclust",
			  "dplyr",
              "DBI",
              "stringr",
              "tibbletime",
              "stringi")

new_packages <- packages[!(packages %in% installed.packages()[,"Package"])]
if(length(new_packages)) install.packages(new_packages)

library(data.table)
library(stats)
library(ggplot2)
library(plyr)
library(dplyr)
library(reshape2)
library(tidyr)
library(sparklyr)
library(fpc)
library(readr)
library(caret)
library(pvclust)
library(dplyr)
library(DBI)
library(stringr)
library(tibbletime)
library(stringi)


to_df <- function(tbl) tbl %>% collect() %>% as.data.frame()

sc_config <- spark_config()
# sc_config$spark.kryoserializer.buffer.max <- '1G'
# sc <- spark_connect(master = "yarn-client", config = sc_config, app_name="claimsv4")
sc <- spark_connect(master = "local", config = sc_config, app_name="SWAT_Analysis")



claimdatIPH <- tbl(sc, "swat.mcla_IPHclaims_qnxt")
claimdatIPH <- claimdatIPH %>% collect()

allcasesIPH = NULL
for (sm in c('CCI','MCE', 'TANF', 'SPD')){

    #dedupe claims that have same startdate
    claimdatIPH_inter <- claimdatIPH %>% filter(segment==sm) %>%
        arrange(carriermemid, provid, providname, startdate) %>%
        group_by(carriermemid, provid, providname, startdate) %>%     
        summarize(enddate = max(enddate),
            totalpaid = sum(totalpaid), nclaims=n_distinct(claimid))
    print(dim(claimdatIPH_inter))
    
    #create cases #5/30 indx has to start with 0 not 1, otherwise 1st and 2nd, 3rd occurrences would be 1,1,2,etc.
    casesIPH <- claimdatIPH_inter %>% 
      group_by(carriermemid, provid) %>%
    mutate(indx = c(0, cumsum(as.numeric(lead(startdate)) >
                     cummax(as.numeric(enddate)))[-n()]),
            totalpaid=totalpaid, nclaims=nclaims) %>%
      group_by(carriermemid, provid, indx) %>%
      summarise(startdate = first(startdate), enddate = last(enddate),
        case_totalpaid = sum(totalpaid), nclaims=sum(nclaims))
    
    casesIPH <- casesIPH %>% mutate(segment=sm)
    
    if(is.null(allcasesIPH)){
        allcasesIPH <- casesIPH
    }else{ allcasesIPH <- rbind(allcasesIPH, casesIPH) }
    print(sm)
}
allcasesIPH <- copy_to(sc, allcasesIPH, overwrite=TRUE)            
allcasesIPH <- allcasesIPH %>% mutate(los = datediff(enddate, startdate)+1) %>%
    mutate(costperday = case_totalpaid/los,
        case_id = paste(carriermemid,provid,segment,as.integer(indx),sep='_'))
        

# #--assign cases back to the claims data, please change table names before running so it doesn't drop my current tables.        
# allcasesIPH <- sdf_copy_to(sc, allcasesIPH, overwrite=TRUE)

# DBI::dbGetQuery(sc,"drop table if exists swat.mcla_iphcases_qnxt")
# DBI::dbGetQuery(sc,"create table swat.mcla_iphcases_qnxt stored as parquet as select * from allcasesIPH")

# claimdatIPH <- sdf_copy_to(sc, claimdatIPH, overwrite=TRUE)

# DBI::dbGetQuery(sc,'drop table if exists swat.mcla_iphclaims_qnxt_withcaseids')
# DBI::dbGetQuery(sc,'create table swat.mcla_iphclaims_qnxt_withcaseids stored as parquet as select c.*, cs.case_id
            # from claimdatIPH c
            # left join allcasesIPH cs
            # on c.carriermemid = cs.carriermemid
            # and c.provid = cs.provid
            # and c.segment = cs.segment
            # and c.startdate <= cs.startdate
            # and c.enddate <= c.enddate')






