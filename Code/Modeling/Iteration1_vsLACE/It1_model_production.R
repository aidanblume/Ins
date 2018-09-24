############################################################################################################
############################################################################################################
##  Title:              It1_model_production.R
##  Description:        Trains a gradient boosting model on the LACE predictor set. 
##                      Choices in this file reflect findings made in It1_model.R.
##  Version Control:    https://dsghe.lacare.org/nblume/Readmissions/tree/master/Code/Modeling/Iteration1_vsLACE
##  Stack:              Use R, 4 vCPU / 32 GiB Memory engine profile 
##  Data Source:        path="hdfs://nameservice1/user/hive/warehouse/nathalie/njb_analytic_set_lace"  
##  Output:             tk report path here
##  Project:            Readmission
##  Authors:            Nathalie Blume
###########################################################################################################
###########################################################################################################





## ENVIRONMENT

#######################
## Control Variables ##
#######################

set.seed(1234) #[*** switch off for deployment]

# [*** SWITCH ***] 
footprint <- 0 # use this if you want to save nothing into HDFS or as a report
#footprint <- 1 # use this if you want to save files with their unique date. Good for creating a record of a production-level model.  

############################
## Load Required Packages ##
############################

packages <- c("data.table",
              "stats",
              "ggplot2",
              "plyr",
              "dplyr",
              "tidyr",
              "reshape2",
              "sparklyr",
              "readr",
              "caret",
#              "glm",
#              "randomForest",
#              "e1071",
              "PerformanceAnalytics",
              "ROCR",
              "pROC",
              "PRROC",
              "R2HTML",
              "gbm",
              "neuropsychology",
              "rtf",
              "DBI",
              "stringr",
              "lubridate")

new_packages <- packages[!(packages %in% installed.packages()[,"Package"])]
if(length(new_packages)) install.packages(new_packages)
  
library(data.table)
library(stats)
library(ggplot2)
library(plyr)
library(dplyr)
library(tidyr)
library(reshape2)
library(sparklyr)
library(readr)
library(caret)
#library(glm)
#library(randomForest)
#library(e1071)
library(PerformanceAnalytics)
library(ROCR)
library(pROC)
library(PRROC)
library(R2HTML)
library(gbm)
library(neuropsychology)
library(rtf)
library(DBI)
library(stringr)
library(lubridate)

#increate max print to 100
options(max.print=100)

#############################
## Set Up Spark Connection ##
#############################
  
sc_config <- spark_config()
sc_config$spark.kryoserializer.buffer.max <- '1G'
sc <- spark_connect(master = 'yarn', config = sc_config, app_name='readmit production')





## DATA

####################################################
## Load Dataset [***APPLY TO SCORING PIPELINE***] ##
####################################################
  
df <- tbl(sc, 'nathalie.prjrea_analytic_set')  %>%
  filter(is.na(segment) || ! segment %in% c('CCI'))  %>%   # Careful of unintended row exclusion: "filter(! segment %in% c('CCI'))" does not keep nulls, whereas this does: df[- which(df$segment=='CCI'), ]
  select(case_id, 
         los, 
         from_er, #acuity
         previousmyocardialinfarction, cerebrovasculardisease, peripheralvasculardisease, diabeteswithoutcomplications,
         congestiveheartfailure, diabeteswithendorgandamage, chronicpulmonarydisease, mildliverorrenaldisease,
         anytumor, dementia, connectivetissuedisease, aids, moderateorsevereliverorrenaldisease, metastaticsolidtumor,
         count_prior6m_er,
         is_followed_by_a_30d_readmit)  %>%  
  collect()
  
df <- data.frame(df)

######################################################################
## Data Invariant Transformations [***APPLY TO SCORING PIPELINE***] ##
######################################################################

# Create new Y as factor
df$V_Target <- factor(df$is_followed_by_a_30d_readmit)

#Don't use: you need to attach the scores back to the cases when preparing the output
## Remove variables not considered for modeling, including key
#df <- df[ , -which(names(df) %in% c("case_id"))]
  
# Rename from_er to acuity, which is the prefered term in LACE
names(df)[names(df) == 'from_er'] <- 'acuity'

#############################################################################################
## Split Dataset & Leave Aside Testset (Includes Balancing)                                ##
##                                                                                         ##
## pair 1: dtrain_orig and dvalid_orig have class imbalance.                               ##
## pair 2: dtrain and dvalid do not have class imbalance because readmits are upsampled.   ##
#############################################################################################
  
# Random sampling, partition data into training (70%), remaining for validation (30%)
inTrain <- createDataPartition(y=df$is_followed_by_a_30d_readmit, p=0.7, list=F)
dtrain_orig <- df[inTrain,]
dvalid_orig <- df[-inTrain,]

# Upsample to correct perceived class imbalance, AFTER training split
# Minority class is randomly sampled with replacement
dtrain_bal <- upSample(dtrain_orig,dtrain_orig$V_Target)
dvalid_bal <- upSample(dvalid_orig,dvalid_orig$V_Target)

# Decide which set to use going forward [*** SWITCH ***]:
  #original
#dtrain <- dtrain_orig
#dvalid <- dvalid_orig
  #balanced
dtrain <- dtrain_bal
dvalid <- dvalid_bal





## DESCRIPTIVE STATISTICS and TRANSFORMATIONS ON TRAINING SET

#############
## Overview
#############

if (footprint) {
  
  # Descriptive on model, training and validation data
    # Note that if Training set was upsampled, it will have more rows than DF set. 
  paste('Model data has',dim(df)[1],'rows and',dim(df)[2], 'columns.',sep=' ')
  paste('Training data has',dim(dtrain)[1],'rows',sep=' ')
  paste('Validation data has',dim(dvalid)[1],'rows',sep=' ')
  print('The following descriptive statistics apply to the training data set.')

  ## Categorical variables
    # Note that there are no categorical variables
  #cat_var <- names(dtrain)[which(sapply(dtrain,is.character))]
  #if (length(cat_var)>0) {
  #    colSums(sapply(df[,cat_var],is.na))
  #} else {
  #    print("No categorical variable found!")
  #}

  # Numeric variables
  numeric_var <- names(df)[which(sapply(df,is.numeric))]

  # Exploratory Data Analysis - Summary statistics
  t(summary(dtrain))

  # Exploratory Data Analysis - Descriptive Statistics
  sumstat <- data.frame(colnm = integer(),
                        varnm = character(),
                        vartype = character(),
                        nmiss = integer(),
                        min   = double(),
                        p1    = double(),
                        p5    = double(),
                        p10   = double(),
                        p25   = double(),
                        med   = double(),
                        avg   = double(),
                        p75   = double(),
                        p90   = double(),
                        p95   = double(),
                        p99   = double(),
                        max   = double(),
                        stringsAsFactors = F)

  for (i in 2:dim(dtrain)[2]) {
    if (class(dtrain[[i]])=="numeric" || class(dtrain[[i]])=="integer") {
    sumstat[i-1,1]  <- i
    sumstat[i-1,2]  <- names(dtrain)[i]
    sumstat[i-1,3]  <- class(dtrain[[i]])
    sumstat[i-1,4]  <- sum(is.na(dtrain[[i]]))
    sumstat[i-1,5]  <- min(dtrain[[i]],na.rm=T)
    sumstat[i-1,6]  <- quantile(dtrain[[i]],0.01,na.rm=T)
    sumstat[i-1,7]  <- quantile(dtrain[[i]],0.05,na.rm=T)
    sumstat[i-1,8]  <- quantile(dtrain[[i]],0.10,na.rm=T)
    sumstat[i-1,9]  <- quantile(dtrain[[i]],0.25,na.rm=T)
    sumstat[i-1,10]  <- median(dtrain[[i]],na.rm=T)
    sumstat[i-1,11] <- mean(dtrain[[i]],na.rm=T)
    sumstat[i-1,12] <- quantile(dtrain[[i]],0.75,na.rm=T)
    sumstat[i-1,13] <- quantile(dtrain[[i]],0.90,na.rm=T)
    sumstat[i-1,14] <- quantile(dtrain[[i]],0.95,na.rm=T)
    sumstat[i-1,15] <- quantile(dtrain[[i]],0.99,na.rm=T)
    sumstat[i-1,16] <- max(dtrain[[i]],na.rm=T)
    }
  }

  # Export descriptive statistics to hive table
    #TK njb does not appear to result in a table in hue
  sdf_copy_to(sc,sumstat,overwrite=TRUE)
  DBI::dbGetQuery(sc, paste('drop table if exists nathalie.', gsub('-','',Sys.Date()), '_prjrea_sumstat', sep=''))
  DBI::dbGetQuery(sc, paste('create table nathalie.', gsub('-','',Sys.Date()), '_prjrea_sumstat as select * from sumstat', sep=''))

}
  
###################
## Missing values
###################

if(footprint){
  
  # Exploratory Data Analysis - Summary of Missing
  totmiss <- round(sum(is.na(dtrain))/(nrow(dtrain)*ncol(dtrain))*100,digits=2)
  is.na(dtrain)
  paste("Total % missing values: ",totmiss,"%",sep='')

  # Exploratory Data Analysis - Plot of Missing if any
  if (totmiss > 0) {
    plot_missing <- function(datain,title=NULL) {
      tmp_df <- as.data.frame(ifelse(is.na(datain),0,1))
      tmp_df <- tmp_df[,order(colSums(tmp_df))]
      data_tmp<- expand.grid(list(x=1:nrow(tmp_df),y=colnames(tmp_df)))
      data_tmp$m <- as.vector(as.matrix(tmp_df))
      data_tmp <- data.frame(x=unlist(data_tmp$x), y=unlist(data_tmp$y),m=unlist(data_tmp$m))
      ggplot(data_tmp) + 
      geom_tile(aes(x=x,y=y,fill=factor(m))) +
      scale_fill_manual(values=c("black","white"),name="Missing\n(0=Yes, 1=No)") +
      theme_light() +
      ylab("") +
      xlab("") +
      ggtitle(title)
    }
    plot_missing(dtrain,title="Plot of Missing Values")
  }  
  
}
  
# TRANSFORMATION
# Remove any row with missing values. 
dtrain <- na.omit(dtrain)
# TK improvement: do value imputation because you want to avoid not scoring patients during the deployment phase.
## TK Here, just assign the modal value to each of the following field:
#           from_er, #acuity
#         previousmyocardialinfarction, cerebrovasculardisease, peripheralvasculardisease, diabeteswithoutcomplications,
#         congestiveheartfailure, diabeteswithendorgandamage, chronicpulmonarydisease, mildliverorrenaldisease,
#         anytumor, dementia, connectivetissuedisease, aids, moderateorsevereliverorrenaldisease, metastaticsolidtumor,

######################
## Near Zero Variance
######################

  #(TURNED OFF) Turned off because LACE requires specific predictor set whether or not it has nzv.
  #Training on alternative models will be unaffected. 
  
  ## Check near-zero of values in every variable
  #nzvar <- nearZeroVar(dtrain, saveMetrics = TRUE)
  #nzvar$varn <- row(nzvar)[,1]
  #nzvar$varnm <- names(dtrain)
  ## nzvar$exclude <- nzvar$zeroVar | nzvar$nzv
  #nzvar$exclude <- nzvar$percentUnique < 1
  #
  #if(footprint) {
  #  
  #  # Export near zero check results to hive table
  #  sdf_copy_to(sc,nzvar,overwrite=TRUE)
  #  DBI::dbGetQuery(sc,"drop table if exists nathalie.readm_eda_nzvar")
  #  DBI::dbGetQuerytQuery(sc,"create table nathalie.readm_eda_nzvar as select * from nzvar")
  #
  #}
  #
  ## TRANSFORMATION
  ## Remove variables without any variance in value
  #exclusionlude<-nzvar[nzvar$exclude,"varnm"]

##############
## Outliers ##
##############

#All variables except LOS and COUNT_PRIOR6M_ERASE binary. 
#NOTE TK: IN ORIGINAL RUN IT WAS INCORRECTLY STATED THAT count_prior6m_er is bounded [1; 4]. 
#THIS WAS TRUE OF THE LACE ALGORITHM BUT SHOULD NOT HAVE BEEN RELEVANT TO ML MODEL_BUILDING.
  
# LOS

  los_stats <- dtrain %>%
    summarize(
      mean_los = mean(los),
      stdev_los = sd(los),
      min_los = min(los),
      max_los = max(los),
      #(TK IMPROVEMENT POSSIBLE) The cutoff below is meant to id extreme outliers only. 
      #LOS varies between medical and mental hospitalizations.
      #At next iteration, primary dx will be available and outliers should be
      #determined by Dx and addressed with within-dx data transformations. 
      cutoff = 20 * sd(los)
    )

if(footprint) {

  # Summary table 
  frequency_distribution <- dtrain %>% 
    group_by(los) %>% 
    summarize(n = n()) %>% 
    arrange(los) %>% collect()

  #Histogram
  ggplot(frequency_distribution, aes( x=los, y=n )) +   
    geom_line()+
    geom_point() +
    geom_vline(xintercept = los_stats$cutoff, colour="red") +
    geom_text(aes(x=los_stats$cutoff, label="\nCutoff at 20 stdev above the mean", y=7500), colour="red", angle=90, text=element_text(size=11))

}
  
  # Transformation
  # Here all LOS is treated the same. 
  # The transformation is to place a ceiling on LOS and to reduce outliers to the ceiling.
  # This does not make a distinction between data entry error and truly long stays.
  # This does not make a distinction between diagnoses where LOS is expected to be greater (e.g. psychiatric hospitalizations)
  # Improvementsprovements in data quality and discrimination by Dx could therefore be made. 
train <- dtrain %>% mutate(los = ifelse(los > los_stats$cutoff, los_stats$cutoff, los))
  
# COUNT_PRIOR6M_ERASE
  
count_prior6m_er_stats <- dtrain %>%
  summarize(
    mean_count_prior6m_er = mean(count_prior6m_er),
    stdev_count_prior6m_er = sd(count_prior6m_er),
    min_count_prior6m_er = min(count_prior6m_er),
    max_count_prior6m_er = max(count_prior6m_er),
    #(TK IMPROVEMENT POSSIBLE) The cutoff below is meant to id extreme outliers only. 
    #At next iteration, primary dx will be available and outliers should be
    #determined by Dx and addressed with within-dx data transformations. 
    cutoff = 20 * sd(count_prior6m_er)
  )

if(footprint) {

  # Summary table 
  frequency_distribution <- dtrain %>% 
    group_by(count_prior6m_er) %>% 
    summarize(n = n()) %>% 
    arrange(count_prior6m_er) %>% collect()

  ##Histogram
  #ggplot(frequency_distribution, aes( x=count_prior6m_er, y=n )) +   
  #  geom_line()+
  #  geom_point() +
  #  geom_vline(xintercept = count_prior6m_er_stats$cutoff, colour="red") +
  #  geom_text(aes(x=count_prior6m_er_stats$cutoff, label="\nCutoff at 20 stdev above the mean", y=7500), colour="red", angle=90, text=element_text(size=11))

}
  
  # TRANSFORMATION
  # Here all count_prior6m_er is treated the same. 
  # The transformation is to place a ceiling on count_prior6m_er and to reduce outliers to the ceiling.
  # This does not make a distinction between data entry error and truly large ER visit counts.
  # This does not make a distinction between diagnoses where count_prior6m_er is expected to be greater.
  # Improvements in data quality and discrimination by Dx could therefore be made. 
dtrain_clean <-  dtrain %>% 
          mutate(los = ifelse(los > los_stats$cutoff, los_stats$cutoff, los)) %>% 
          mutate(count_prior6m_er = ifelse(count_prior6m_er > count_prior6m_er_stats$cutoff, count_prior6m_er_stats$cutoff, count_prior6m_er))
  
#############
## SCALE
#############
  
## Scale final modeling variables for modeling
##Not done for this iteration
#final<-scale(model[,!colnames(model) %in% exclude],center=T,scale=T)





## PIPING THE DATA TRANSFORMATION & APPLYING TO TEST SET
  
#################################
## Validation Set Transformation
#################################

# The value of all transformation parameters were determined with the training data set
# (TK SWITCH) Remember to omit transformations that were omitted with the training set
  
dvalid_clean <- dvalid %>%
          na.omit() %>%
          mutate(los = ifelse(los > los_stats$cutoff, los_stats$cutoff, los)) %>% 
          mutate(count_prior6m_er = ifelse(count_prior6m_er > count_prior6m_er_stats$cutoff, count_prior6m_er_stats$cutoff, count_prior6m_er))  





## MODEL TRAINING

###########################
## STATE TRAINING METRIC
###########################

# We could set the training performance metric as Kappa (instead of Accuracy) or write a 
# function for AUPRC. The solution below applies a cost function that renders FN 10 times
# more costly than FP. 
  
# Cost Function approach #
  
# Assume cost for false positive and false negative
# This factor may be generated from actual cost (cost of care given) incurred from false positive,
# comparing with opportunity cost (in-patient hospitalization cost) of false negative
    
# Hard coded cost_fn_fp_ratio=10 due to it being in function
# In the future will need to have it refer to cost_fn_fp_ratio directly
# Variable cost_fn_fp_ratio is still used in reporting performance. 
cost_fn_fp_ratio <- 10
costSum <- function(data, lev = NULL, model = NULL) {
  require("e1071")
  out <- (unlist(e1071::classAgreement(table(data$obs, data$pred))) [c("diag","kappa")])
  names(out) <- c("Accuracy", "Kappa")
  costSum_val <- sum(ifelse(data$pred==data$obs,0,ifelse(data$pred==1,1,10)))/length(data$pred)
  out2 <- c(out, costSum_val)
  names(out2) <- c("Accuracy", "Kappa", "CostSum")
  out2
}

# Model run setting: 5-fold cross validation on the training sample (70%)
#fitControl  <- trainControl(method="repeatedcv", number=5, repeats=5, savePrediction=T, summaryFunction = costSum)  
fitControl  <- trainControl(method="cv", number=5, savePrediction=T, summaryFunction = costSum)  

# TK other approaches, from njb code
# Cost Function approach #
# Cost Function approach #
# Cost Function approach #

######################
## Gradient Boosting
######################

#gbmGrid <- expand.grid(interaction.depth=c(1, 3, 5), n.trees = (0:50)*50,
#                   shrinkage=c(0.01, 0.001),
#                   n.minobsinnode=10)

gbmModel <- train(V_Target ~ los + acuity + cerebrovasculardisease +
                  peripheralvasculardisease + diabeteswithoutcomplications + congestiveheartfailure +
                  diabeteswithendorgandamage + chronicpulmonarydisease + mildliverorrenaldisease +
                  anytumor + dementia + connectivetissuedisease + aids + metastaticsolidtumor + count_prior6m_er,
                  data=dtrain_clean, method = "gbm", metric = "CostSum", maximize=F, trControl = fitControl,
                  verbose=FALSE)

# Accuracy and Kappa on training dataset
ak_gbm<-gbmModel$results
ak_gbm<-round(ak_gbm[ak_gbm$CostSum == min(ak_gbm[,"CostSum"]),c("Accuracy","Kappa","CostSum")],4)

# Variable Importance Table on training dataset
vi_gbm<-varImp(gbmModel)

  
##################################################
## Performance metrics: Test/Validation dataset ##
##################################################
  
# Score on validation/test sample only
pred_gbm <- predict(gbmModel,dvalid_clean)
  
# Confusion Matrix
CM_GBM <- confusionMatrix(pred_gbm,dvalid_clean$V_Target,positive='1')
  
# AUPRC
pr_gbm <- pr.curve(scores.class0 = as.numeric(as.character(pred_gbm[dvalid_clean$V_Target=='1'])), 
                  scores.class1 = as.numeric(as.character(pred_gbm[dvalid_clean$V_Target=='0'])), curve = T)
 
# Diagnostic Metrics
diag_gbm<-round(c(CM_GBM$overall[2],
                 pr_gbm$auc.integral,
                 CM_GBM$byClass[c(3,6)],
                round((CM_GBM$table["0","1"]*cost_fn_fp_ratio+CM_GBM$table["1","0"]*1)/sum(CM_GBM$table),4)
                ),4)
  
names(diag_gbm)<-c("Kappa","AUPRC","Precision","Recall", "Cost")
  


  
    
## SCORING
  
############################
## Get and Transform File ##
############################

# load dataset: [*** Keep CCI segment ***]
df_scoring <- tbl(sc, 'nathalie.prjrea_analytic_set')  %>%
  select(case_id, 
         los, 
         from_er, 
         previousmyocardialinfarction, cerebrovasculardisease, peripheralvasculardisease, diabeteswithoutcomplications,
         congestiveheartfailure, diabeteswithendorgandamage, chronicpulmonarydisease, mildliverorrenaldisease,
         anytumor, dementia, connectivetissuedisease, aids, moderateorsevereliverorrenaldisease, metastaticsolidtumor,
         count_prior6m_er,
         is_followed_by_a_30d_readmit,
         segment)  %>%  
  collect()
  
df_scoring <- data.frame(df_scoring)

# Transformations 
df_scoring$V_Target <- factor(df_scoring$is_followed_by_a_30d_readmit)
names(df_scoring)[names(df_scoring) == 'from_er'] <- 'acuity'
dscoring_clean <- df_scoring %>%
          na.omit() %>%
          mutate(los = ifelse(los > los_stats$cutoff, los_stats$cutoff, los)) %>% 
          mutate(count_prior6m_er = ifelse(count_prior6m_er > count_prior6m_er_stats$cutoff, count_prior6m_er_stats$cutoff, count_prior6m_er))  

#####################
## Score all cases ##
#####################
 
#Predict
pred_gbm_all <- predict(gbmModel,dscoring_clean)

#Add predictions as a variable/column to the full data set
dscoring_clean$gbm_pred <- pred_gbm_all

#Performance Metrics



#Export the scores as case:prediction pairs under the gradient boosting model. 
dc <- dscoring_clean %>% select(case_id, gbm_pred)
copy_to(sc, dc, overwrite=TRUE)
DBI::dbGetQuery(sc, "drop table if exists nathalie.prjrea_Predictions")
DBI::dbGetQuery(sc, "create table nathalie.prjrea_Predictions stored as parquet as select * from dc")

#Copy again but with dates, for posterity
if(footprint) {

  DBI::dbGetQuery(sc, paste('drop table if exists nathalie.', gsub('-','',Sys.Date()), '_prjrea_predictions', sep=''))
  DBI::dbGetQuery(sc, paste('create table nathalie.', gsub('-','',Sys.Date()), '_prjrea_predictions as select * from dc', sep=''))

}
  

  
  
## STOP THE SPARK SESSION

spark_disconnect(sc)
