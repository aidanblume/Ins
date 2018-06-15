############################################################################################################
############################################################################################################
##
##  Title:              It1_model.R
##  Description:        Trains several models and contrasts them to the LACE index. 
##  Version Control:    https://dsghe.lacare.org/nblume/Readmissions/tree/master/Code/Modeling/Iteration1_vsLACE
##  Data Source:        path="hdfs://nameservice1/user/hive/warehouse/nathalie/njb_analytic_set_lace"  
##  Output:             tk report names here
##  Project:            Readmission
##  Authors:            Nathalie Blume, Qing Sun (Consultant)
##  Last Touched:       May 23, 2018
##
###########################################################################################################
###########################################################################################################


set.seed(1234) #(TK SEED)

  
  


  

## LOAD REQUIRED PACKAGES

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
              "randomForest",
              "e1071",
              "PerformanceAnalytics",
              "ROCR",
              "pROC",
              "PRROC",
              "R2HTML",
              "gbm",
              "neuropsychology",
              "rtf")

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
library(randomForest)
library(e1071)
library(PerformanceAnalytics)
library(ROCR)
library(pROC)
library(PRROC)
library(R2HTML)
library(gbm)
library(neuropsychology)
library(rtf)

#increate max print to 100
options(max.print=100)

  
  


  

## DATA IMPORT
  
#set up spark connection
sc_config <- spark_config()
sc_config$spark.kryoserializer.buffer.max <- '1G'
sc <- spark_connect(master = "yarn", config = sc_config)
#sc <- spark_connect(master = "local", config = sc_config)

# Import the **njb_analytic_set** table as a Spark Data Frame
# Make sure column 1 is key - used row_number() in SQL but dropped before export results # njb return to this for action
  #njb: using dplyer mutate()
sc_model <- spark_read_parquet(sc, name="njb_analytic_set_lace",
        path="hdfs://nameservice1/user/hive/warehouse/nathalie/njb_analytic_set_lace",
        header=T,memory=F)
#sc_model %>% 
#  mutate(cin_no = unlist(lapply(cin_no, function(e) rawToChar(e))))
#  mutate(severity = unlist(lapply(severity, function(e) rawToChar(e))))
#  mutate(aprdrg = unlist(lapply(aprdrg, function(e) rawToChar(e))))
model_raw<-collect(sc_model)

# Fix fields imported as list of raw values from spark_read_parquet
var_to_fix <- which(sapply(model_raw,is.list))
fixed <- vector(mode="character",length=dim(model_raw)[1])
for (j in var_to_fix) {
  for (i in 1:dim(model_raw)[1]) {
    fixed[i] <- rawToChar(model_raw[,j][[1]][[i]])
  }
  model_raw[,j]<-fixed
}
  
# Remove CCI
  #Members in the CCI line of business do not always submit hospitalization claims with us.
  #Therefore their readmission rates are artificially low in our data. 
  
model<-model_raw[which(model_raw$cci != 1),]

#########################################################################
# Alternative to Spark import - Import file locally via CSV
# 2017 all data
#model <- read.csv(file="query-impala-25596.csv", header=T, sep=",")
# 2017 exclude CCI
#model <- read.csv(file="query-hive-27290.csv", header=T, sep=",")
#########################################################################

  
  


  

## DATA INVARIANT TRANSFORMATIONS

# Create new Y as factor
model$V_Target <- factor(model$is_followed_by_a_30d_readmit)

# Remove variables not considered for modeling, including key
model <- model[ , -which(names(model) %in% c("cin_no", "cci", "is_followed_by_death_or_readmit"))]

  
  


  

## SPLIT DATA SET & LEAVE ASIDE TEST SET
  
#######################################
## pair 1: dtrain_orig and dvalid_orig have class imbalance.
## pair 2: dtrain and dvalid do not have class imbalance because readmits are upsampled.
#######################################
  
# Random sampling, partition data into training (70%), remaining for validation (30%)
inTrain <- createDataPartition(y=model$is_followed_by_a_30d_readmit, p=0.7, list=F)
dtrain_orig <- model[inTrain,]
dvalid_orig <- model[-inTrain,]

# Upsample to correct perceived class imbalance, AFTER training split
# Minority class is randomly sampled with replacement
dtrain_bal <- upSample(dtrain_orig,dtrain_orig$V_Target)
dvalid_bal <- upSample(dvalid_orig,dvalid_orig$V_Target)

# All samples
model %>% count(is_followed_by_a_30d_readmit)
# Training sample (70%)
dtrain_orig %>% count(is_followed_by_a_30d_readmit)
dtrain_bal %>% count(is_followed_by_a_30d_readmit)
# Validation sample (30%)
dvalid_orig %>% count(is_followed_by_a_30d_readmit)
dvalid_bal %>% count(is_followed_by_a_30d_readmit)

# Plot target variable
# All samples
plot(factor(model$is_followed_by_a_30d_readmit))
# Training sample (70%), prior to up sample
plot(factor(dtrain_orig$is_followed_by_a_30d_readmit))
# Validation sample (30%), prior to up sample
plot(factor(dvalid_orig$is_followed_by_a_30d_readmit))

# Decide which set to use going forward (TK SWITCH):
  #original
#dtrain <- dtrain_orig
#dvalid <- dvalid_orig
  #balanced
dtrain <- dtrain_bal
dvalid <- dvalid_bal
  
# Not needed for now
# Convert all numeric predictors to factor (categorical) for classification models
#f_names<-c("los","acuity", "cerebrovasculardisease", "peripheralvasculardisease", 
#           "diabeteswithoutcomplications", "congestiveheartfailure", "diabeteswithendorgandamage", 
#           "chronicpulmonarydisease", "mildliverorrenaldisease", "anytumor", "dementia", 
#           "connectivetissuedisease", "aids", "metastaticsolidtumor", "er_visits",
#           "is_followed_by_a_30d_readmit","is_followed_by_death_or_readmit")
#dtrain[,f_names]<-lapply(dtrain[,f_names],as.character)
#dtrain$is_followed_by_a_30d_readmit<-ifelse(dtrain$is_followed_by_a_30d_readmit==1, "Y","N")
# dtrain[sapply(dtrain,is.numeric)]<-lapply(dtrain[sapply(dtrain,is.numeric)],as.factor)

  
  


  

## DESCRIPTIVE STATISTICS and TRANSFORMATIONS ON TRAINING SET

#############
## Overview
#############
  
# Descriptive on model, training and validation data
  # Note that if Training set was upsampled, it will have more rows than Model set. 
paste('Model data has',dim(model)[1],'rows and',dim(model)[2], 'columns.',sep=' ')
paste('Training data has',dim(dtrain)[1],'rows',sep=' ')
paste('Validation data has',dim(dvalid)[1],'rows',sep=' ')
print('The following descriptive statistics apply to the training data set.')

# Categorical variables
# cat_var <- names(model)[which(sapply(model,is.character))]
# if (length(cat_var)>0) {
#  colSums(sapply(model[,cat_var],is.na))
# } else {
#   print("No categorical variable found!")
# }

# Numeric variables
# numeric_var <- names(model)[which(sapply(model,is.numeric))]
  
# Exploratory Data Analysis - Summary statistics
# Further expanded for all numeric and integer variables below
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
DBI::dbGetQuery(sc,"drop table if exists nathalie.readm_eda_sumstat")
DBI::dbGetQuery(sc,"create table nathalie.readm_eda_sumstat as select * from sumstat")

#############
## Anomalies
#############

#Leslie Seltzer (subject matter expert) confirms diabetes coding is correct. 
#Why 0 cases of diabetes w/ end organ damage? 

#Leslie confirms that AIDS is no longer used as a diagnostic category. 
#Instead the specific ailment (to which HIV may have made the member more vulnerable) 
#is coded. Note that changes in clinical use of codes, such as no longer using AIDS codes,
#weaken the predictive power of LACE compared to its original implementation. Our trained 
#models are weakened as well.

###################
## Missing values
###################

# Exploratory Data Analysis - Summary of Missing
totmiss <- round(sum(is.na(dtrain))/(nrow(dtrain)*ncol(dtrain))*100,digits=2)
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
  
# TRANSFORMATION
# Remove any row with missing values. 
dtrain <- na.omit(dtrain)

######################
## Near Zero Variance
######################

  #(TK TURNED OFF) Turned off because LACE requires specific predictor set whether or not it has nzv.
  #Training on alternative models will be unaffected. 
  
## Check near-zero of values in every variable
#nzvar <- nearZeroVar(dtrain, saveMetrics = TRUE)
#nzvar$varn <- row(nzvar)[,1]
#nzvar$varnm <- names(dtrain)
## nzvar$exclude <- nzvar$zeroVar | nzvar$nzv
#nzvar$exclude <- nzvar$percentUnique < 1
#
## Export near zero check results to hive table
#sdf_copy_to(sc,nzvar,overwrite=TRUE)
#DBI::dbGetQuery(sc,"drop table if exists nathalie.readm_eda_nzvar")
#DBI::dbGetQuery(sc,"create table nathalie.readm_eda_nzvar as select * from nzvar")
#
## TRANSFORMATION
## Remove variables without any variance in value
#exclude<-nzvar[nzvar$exclude,"varnm"]

############
## Outliers
############

#All variables except LOS and ER_VISITS are binary. ER_VISITS is bounded [1; 4]. Only LOS may have outliers.
  
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

# TRANSFORMATION
# Here all LOS is treated the same. 
# The transformation is to place a ceiling on LOS and to reduce outliers to the ceiling.
# This does not make a distinction between data entry error and truly long stays.
# This does not make a distinction between diagnoses where LOS is expected to be greater (e.g. psychiatric hospitalizations)
# Improvements in data quality and discrimination by Dx could therefore be made. 
train <- dtrain %>% mutate(los = ifelse(los > los_stats$cutoff, los_stats$cutoff, los))

#############
## SCALE
#############
  
## Scale final modeling variables for modeling
##Not done for this iteration
#final<-scale(model[,!colnames(model) %in% exclude],center=T,scale=T)

#################################
## Validation Set Transformation
#################################

# The value of all transformation parameters were determined with the training data set
# (TK SWITCH) Remember to omit transformations that were omitted with the training set
  
dvalid <- na.omit(dvalid)
#exclude<-nzvar[nzvar$exclude,"varnm"]
dvalid <- dvalid %>% mutate(los = ifelse(los > los_stats$cutoff, los_stats$cutoff, los))

  
  


  

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
  
#######################
## Logistic Regression
#######################
  
# No tuning parameter for glm in caret (ref: https://stackoverflow.com/questions/47822694/logistic-regression-tuning-parameter-grid-in-r-caret-package).
# TK njb apply tuning grid from Brandon, here and in other training. 
# TK njb Consider using a different approach to logistic regression for better chance to optimize model

logitReg <- train(V_Target ~ los + acuity + cerebrovasculardisease +
                  peripheralvasculardisease + diabeteswithoutcomplications + congestiveheartfailure +
                  diabeteswithendorgandamage + chronicpulmonarydisease + mildliverorrenaldisease +
                  anytumor + dementia + connectivetissuedisease + aids + metastaticsolidtumor + er_visits,
                  data=dtrain, method="glm", metric="CostSum", maximize=F, family=binomial, trControl=fitControl)

#Accuracy and Kappa on training dataset
# tk njb why min() below? Are we selecting the best of a series? If so why on CostSum (and why altogether)?
  
ak_lr<-logitReg$results
  
ak_lr<-round(ak_lr[ak_lr$CostSum == min(ak_lr[,"CostSum"]),c("Accuracy","Kappa","CostSum")],4)

#Variable Importance Table on training dataset

vi_lr<-varImp(logitReg)

# Score on validation sample only
  
pred_lr <- predict(logitReg,dvalid)
  
# Confusion Matrix
  
CM_LR <- confusionMatrix(pred_lr,dvalid$V_Target,positive='1') 
  
# Performance metrics
  
# AUPRC (area under the precision-recall curve)
  
pr_lr <- pr.curve(scores.class0 = as.numeric(as.character(pred_lr[dvalid$V_Target=='1'])), 
                  scores.class1 = as.numeric(as.character(pred_lr[dvalid$V_Target=='0'])), curve = T)

# Diagnostic Metrics: Kappa, AUPRC, Precision, Recall, Cost (only to show effect of training metric)
  
diag_lr<-round(c(CM_LR$overall[2],
                 pr_lr$auc.integral,
                 CM_LR$byClass[c(3,6)],
                 round((CM_LR$table["0","1"]*cost_fn_fp_ratio+CM_LR$table["1","0"]*1)/sum(CM_LR$table),4)
                ),4)
  
names(diag_lr)<-c("Kappa","AUPRC","Precision","Recall", "Cost")

# Output chart for further reporting

png("Readmissions/Docs/Model/Model_1/Graphs/auprc_lr.png")
plot(pr_lr,main="AUPRC Logistic Regression")
dev.off()

## Old -- we don't need to report so many metrics
#
## Signal Processing: njb: we are not using this
## AUROC
#roc_lr <- roc.curve(scores.class0 = as.numeric(as.character(pred_lr[dvalid$V_Target=='1'])), 
#                    scores.class1 = as.numeric(as.character(pred_lr[dvalid$V_Target=='0'])), curve=T)
#
##Validation data: d-prime 
#n_hit <- CM_LR$table[2,2]
#n_miss <- CM_LR$table[1,2]
#n_fa <- CM_LR$table[2,1]
#n_cr <- CM_LR$table[1,1]
#tmp <- dprime(n_hit, n_miss, n_fa, n_cr) #from neuropsychology package
#dprime_lr <- tmp[1]
#
## Diagnostic Metrics
#diag_lr<-round(c(CM_LR$overall[1:2],
#                 pr_lr$auc.integral,
#                 CM_LR$byClass[c(3,6,7,11)],
#                 roc_lr$auc,
#                 dprime_lr$dprime,
#                round((CM_LR$table["0","1"]*cost_fn_fp_ratio+CM_LR$table["1","0"]*1)/sum(CM_LR$table),4)
#                ),4)
#names(diag_lr)<-c("Accuracy","Kappa","AUPRC Integral","Precision","Recall","F1","Balanced Accuracy","AUROC","D-Prime","Cost")

##################
## Decision Tree
##################

dTree <- train(V_Target ~ los + acuity + cerebrovasculardisease +
                  peripheralvasculardisease + diabeteswithoutcomplications + congestiveheartfailure +
                  diabeteswithendorgandamage + chronicpulmonarydisease + mildliverorrenaldisease +
                  anytumor + dementia + connectivetissuedisease + aids + metastaticsolidtumor + er_visits,
                  data=dtrain, method="rpart", metric="CostSum", maximize=F, trControl=fitControl)  

# Accuracy and Kappa on training dataset
  
ak_dt<-dTree$results
  
ak_dt<-round(ak_dt[ak_dt$CostSum == min(ak_dt[,"CostSum"]),c("Accuracy","Kappa","CostSum")],4)

# Variable Importance Table on training dataset
  
vi_dt<-varImp(dTree)

# Score on validation sample only
  
pred_dt <- predict(dTree,dvalid)
  
# Confusion Matrix
  
CM_DT <- confusionMatrix(pred_dt,dvalid$V_Target,positive='1') 
  
# Performance metrics
  
# AUPRC
  
pr_dt <- pr.curve(scores.class0 = as.numeric(as.character(pred_dt[dvalid$V_Target=='1'])), 
                  scores.class1 = as.numeric(as.character(pred_dt[dvalid$V_Target=='0'])), curve = T)
 
# Diagnostic Metrics
  
diag_dt<-round(c(CM_DT$overall[2],
                 pr_dt$auc.integral,
                 CM_DT$byClass[c(3,6)],
                round((CM_DT$table["0","1"]*cost_fn_fp_ratio+CM_DT$table["1","0"]*1)/sum(CM_DT$table),4)
                ),4)

names(diag_dt)<-c("Kappa","AUPRC","Precision","Recall", "Cost")
  
# Output chart for further reporting
  
png("Readmissions/Docs/Model/Model_1/Graphs/auprc_dt.png")

plot(pr_dt,main="AUPRC Decision Tree")

dev.off()    

##################
## Random Forest
##################
  #tk check that the random forest you use is from caret and not from e1071

rForest <- train( V_Target ~ los + acuity + cerebrovasculardisease +
                  peripheralvasculardisease + diabeteswithoutcomplications + congestiveheartfailure +
                  diabeteswithendorgandamage + chronicpulmonarydisease + mildliverorrenaldisease +
                  anytumor + dementia + connectivetissuedisease + aids + metastaticsolidtumor + er_visits,
                 data=dtrain, method="rf", metric="CostSum", maximize=F, ntree=500, 
                 trControl=fitControl)  

# Accuracy and Kappa on training dataset
  
ak_rf<-rForest$results
  
ak_rf<-round(ak_rf[ak_rf$CostSum == min(ak_rf[,"CostSum"]),c("Accuracy","Kappa","CostSum")],4)

# Variable Importance Table on training dataset
  
vi_rf<-varImp(rForest)
  
# Score on validation sample only
  
pred_rf <- predict(rForest,dvalid)
  
# Confusion Matrix
  
CM_RF <- confusionMatrix(pred_rf,dvalid$V_Target,positive='1') 
  
# Performance metrics
  
# AUPRC
  
pr_rf <- pr.curve(scores.class0 = as.numeric(as.character(pred_rf[dvalid$V_Target=='1'])), 
                  scores.class1 = as.numeric(as.character(pred_rf[dvalid$V_Target=='0'])), curve = T)
 
# Diagnostic Metrics
  
diag_rf<-round(c(CM_RF$overall[2],
               pr_rf$auc.integral,
               CM_RF$byClass[c(3,6)],
              round((CM_RF$table["0","1"]*cost_fn_fp_ratio+CM_RF$table["1","0"]*1)/sum(CM_RF$table),4)
              ),4)
  
names(diag_rf)<-c("Kappa","AUPRC","Precision","Recall", "Cost")
    
# Output chart for further reporting
  
png("Readmissions/Docs/Model/Model_1/Graphs/auprc_rf.png")

plot(pr_dt,main="AUPRC Random Forest")

dev.off()    

######################
## Gradient Boosting
######################

#gbmGrid <- expand.grid(interaction.depth=c(1, 3, 5), n.trees = (0:50)*50,
#                   shrinkage=c(0.01, 0.001),
#                   n.minobsinnode=10)

gbmModel <- train(V_Target ~ los + acuity + cerebrovasculardisease +
                  peripheralvasculardisease + diabeteswithoutcomplications + congestiveheartfailure +
                  diabeteswithendorgandamage + chronicpulmonarydisease + mildliverorrenaldisease +
                  anytumor + dementia + connectivetissuedisease + aids + metastaticsolidtumor + er_visits,
                  data=dtrain, method = "gbm", metric = "CostSum", maximize=F, trControl = fitControl,
                  verbose=FALSE)

# Accuracy and Kappa on training dataset
  
ak_gbm<-gbmModel$results
  
ak_gbm<-round(ak_gbm[ak_gbm$CostSum == min(ak_gbm[,"CostSum"]),c("Accuracy","Kappa","CostSum")],4)

# Variable Importance Table on training dataset
  
vi_gbm<-varImp(gbmModel)

# Score on validation sample only
  
pred_gbm <- predict(gbmModel,dvalid)
  
# Confusion Matrix
  
CM_GBM <- confusionMatrix(pred_gbm,dvalid$V_Target,positive='1')

# Performance metrics
  
# AUPRC
  
pr_gbm <- pr.curve(scores.class0 = as.numeric(as.character(pred_gbm[dvalid$V_Target=='1'])), 
                  scores.class1 = as.numeric(as.character(pred_gbm[dvalid$V_Target=='0'])), curve = T)
 
# Diagnostic Metrics
  
diag_gbm<-round(c(CM_GBM$overall[2],
                 pr_gbm$auc.integral,
                 CM_GBM$byClass[c(3,6)],
                round((CM_GBM$table["0","1"]*cost_fn_fp_ratio+CM_GBM$table["1","0"]*1)/sum(CM_GBM$table),4)
                ),4)
  
names(diag_gbm)<-c("Kappa","AUPRC","Precision","Recall", "Cost")
  
# Output chart for further reporting
  
png("Readmissions/Docs/Model/Model_1/Graphs/auprc_gbm.png")

plot(pr_dt,main="AUPRC Gradient Boost")

dev.off()    
  
###################
## Neural Network
###################

nnetModel <- train(V_Target ~ los + acuity + cerebrovasculardisease +
                  peripheralvasculardisease + diabeteswithoutcomplications + congestiveheartfailure +
                  diabeteswithendorgandamage + chronicpulmonarydisease + mildliverorrenaldisease +
                  anytumor + dementia + connectivetissuedisease + aids + metastaticsolidtumor + er_visits,
                  data=dtrain, method="nnet", metric = "CostSum", maximize=F, trControl = fitControl)

# Accuracy and Kappa on training dataset
  
ak_nn<-nnetModel$results
  
ak_nn<-round(ak_nn[ak_nn$CostSum == min(ak_nn[,"CostSum"]),c("Accuracy","Kappa","CostSum")],4)

# Variable Importance Table on training dataset
  
vi_nn<-varImp(nnetModel)
  
# Score on validation sample only
  
pred_nn <- predict(nnetModel,dvalid)
  
# Confusion Matrix
  
CM_NN <- confusionMatrix(pred_nn,dvalid$V_Target,positive='1') 
  
# Performance metrics
  
# AUPRC
  
pr_nn <- pr.curve(scores.class0 = as.numeric(as.character(pred_nn[dvalid$V_Target=='1'])), 
                  scores.class1 = as.numeric(as.character(pred_nn[dvalid$V_Target=='0'])), curve = T)
 
# Diagnostic Metrics
  
diag_nn<-round(c(CM_NN$overall[2],
                 pr_nn$auc.integral,
                 CM_NN$byClass[c(3,6)],
                 round((CM_NN$table["0","1"]*cost_fn_fp_ratio+CM_NN$table["1","0"]*1)/sum(CM_NN$table),4)
                ),4)
  
names(diag_nn)<-c("Kappa","AUPRC","Precision","Recall", "Cost")
  
# Output chart for further reporting
  
png("Readmissions/Docs/Model/Model_1/Graphs/auprc_nn.png")

plot(pr_nn,main="AUPRC Neural Net")

dev.off()    

##################################
## Ensemble Model: Majority Vote
##################################

# Create a data frame for voting
  
tmp_df <- data.frame(pred_lr, pred_dt, pred_rf, pred_gbm, pred_nn)
    
## start validity check: with our data type issues, 
## Qing observed elsewhere that +1 was added to as.numeric(as.character transformation)  
#tmp_df2 <- mutate_all(tmp_df, function(x) as.numeric(as.character(x)))
#test_rows <- which(pred_lr == 1)
#unique(tmp_df[test_rows, ] == tmp_df2[test_rows, ])
#rm(tmp_df2)
## end validity check: all rows are TRUEx4. Transformation is correct; +1 is not added on top
  
tmp_df <- mutate_all(tmp_df, function(x) as.numeric(as.character(x)))

# Majority Vote: Score validation set based on majority rule (3 out of 5)

tmp_df$sum <- tmp_df$pred_lr + tmp_df$pred_dt +tmp_df$pred_rf + tmp_df$pred_gbm + tmp_df$pred_nn

pred_esm <- ifelse(tmp_df$sum >= 3, 1, 0)

# Confusion Matrix
  
CM_ESM <- confusionMatrix(as.factor(pred_esm),dvalid$V_Target,positive='1')
  
# Performance metrics
  
# AUPRC
  
pr_esm <- pr.curve(scores.class0 = as.numeric(as.character(pred_esm[dvalid$V_Target=='1'])), 
                  scores.class1 = as.numeric(as.character(pred_esm[dvalid$V_Target=='0'])), curve = T)
 
# Diagnostic Metrics
  
diag_esm<-round(c(CM_ESM$overall[2],
                 pr_esm$auc.integral,
                 CM_ESM$byClass[c(3,6)],
                round((CM_ESM$table["0","1"]*cost_fn_fp_ratio+CM_ESM$table["1","0"]*1)/sum(CM_ESM$table),4)
                ),4)

names(diag_esm)<-c("Kappa","AUPRC","Precision","Recall", "Cost")
  
# Output chart for further reporting
  
png("Readmissions/Docs/Model/Model_1/Graphs/auprc_esm.png")

plot(pr_nn,main="AUPRC Ensemble Majority Vote")

dev.off()    

#######################################
## Ensemble Model: "Black Ball Wins" Vote (was Qing's "Maximum Prediction" model)
#######################################

# Score validation set with rule: 
# "If any model predicts positive/will be readmitted, then ensemble predicts same."
# Due to low cost of False Positive relative to False Negative,it is better to produce as many positive as possible

# Vote
  
pred_esbbw <- ifelse(tmp_df$sum > 0, 1, 0)
  
# Confusion Matrix
  
CM_ESBBW <- confusionMatrix(as.factor(pred_esbbw),dvalid$V_Target,positive='1')
  
# Performance metrics
  
# AUPRC
  
pr_esbbw <- pr.curve(scores.class0 = as.numeric(as.character(pred_esbbw[dvalid$V_Target=='1'])), 
                  scores.class1 = as.numeric(as.character(pred_esbbw[dvalid$V_Target=='0'])), curve = T)
 
# Diagnostic Metrics
  
diag_esbbw<-round(c(CM_ESBBW$overall[2],
                 pr_esbbw$auc.integral,
                 CM_ESBBW$byClass[c(3,6)],
                round((CM_ESBBW$table["0","1"]*cost_fn_fp_ratio+CM_ESBBW$table["1","0"]*1)/sum(CM_ESBBW$table),4)
                ),4)
names(diag_esbbw)<-c("Kappa","AUPRC","Precision","Recall", "Cost")
  
# Output chart for further reporting
  
png("Readmissions/Docs/Model/Model_1/Graphs/auprc_esbbw.png")

plot(pr_nn,main="AUPRC Ensemble Black Ball Wins")

dev.off()    

  
  


  

## BASELINE MODEL PERFORMANCE (VALIDATION): LACE

#################################################
# Generate predictions based on LACE scores 
# Use criterion >= 10 
# (in LACE methodology, low+moderate risk vs. high risk). 
# ALT: Assign 0, 1 values to cases by descreasing LACE-score order, 
# where top x% get 1, otherwise get 0; 
# x is determined by the proportion of 1s in the trained model predictions.
#################################################

# Generate LACE scores
  
# Note: Adding 1 to LOS to align with LACE definition
dvalid$los_lace <- dvalid$los + 1
  
dvalid$score_l = ifelse(dvalid$los_lace <= 3, dvalid$los_lace, 
                       ifelse(dvalid$los_lace <= 6, 4, 
                              ifelse(dvalid$los_lace <= 13, 5, 
                                     ifelse(dvalid$los_lace>=14, 7, 0)
                                    )
                             )
                      )

dvalid$score_a <- dvalid$acuity * 3

dvalid$score_c <- dvalid$previousmyocardialinfarction * 1 + dvalid$cerebrovasculardisease * 1 +
                 dvalid$peripheralvasculardisease * 1 + dvalid$diabeteswithoutcomplications * 1 +
                 dvalid$congestiveheartfailure * 2 + dvalid$diabeteswithendorgandamage * 2 +
                 dvalid$chronicpulmonarydisease * 2 + dvalid$mildliverorrenaldisease * 2 +
                 dvalid$anytumor * 2 +
                 dvalid$dementia * 3 + dvalid$connectivetissuedisease * 3 +
                 dvalid$aids * 4 + dvalid$moderateorsevereliverorrenaldisease * 4 +
                 dvalid$metastaticsolidtumor * 6

dvalid$score_e = ifelse(dvalid$er_visits>4, 4, dvalid$er_visits)

dvalid$score_lace = dvalid$score_l + dvalid$score_a + dvalid$score_c + dvalid$score_e

#convoluted way to code freq distr
frequency_distribution <- dvalid %>% 
  group_by(score_lace) %>% 
  summarize(n = n()) %>% 
  arrange(score_lace) %>% collect()

#The following line is a simpler way to code frequency_distribution
frequency_distribution <- dvalid %>% count(score_lace)
  
ggplot(frequency_distribution, aes( x=score_lace, y=n )) +   
  geom_line()+
  geom_point()   
  
pred_lace<-(dvalid$score_lace>=10)*1
  
# Confusion Matrix
  
CM_LACE <- confusionMatrix(as.factor(pred_lace),dvalid$V_Target,positive='1') 
 
# Performance metrics
  
# AUPRC
  
pr_lace <- pr.curve(scores.class0 = as.numeric(as.character(pred_lace[dvalid$V_Target=='1'])), 
                  scores.class1 = as.numeric(as.character(pred_lace[dvalid$V_Target=='0'])), curve = T)
 
# Diagnostic Metrics
  
diag_lace<-round(c(CM_LACE$overall[2],
                 pr_lace$auc.integral,
                 CM_LACE$byClass[c(3,6)],
                round((CM_LACE$table["0","1"]*cost_fn_fp_ratio+CM_LACE$table["1","0"]*1)/sum(CM_LACE$table),4)
                ),4)
  
names(diag_lace)<-c("Kappa","AUPRC","Precision","Recall", "Cost")
  
# Output chart for further reporting
  
png("Readmissions/Docs/Model/Model_1/Graphs/auprc_lace.png")

plot(pr_nn,main="AUPRC Baseline - LACE")

dev.off()    

  
  


  

## FUTURE AVENUES FOR IMPROVEMENT  

  
###################################
## REPORT
###################################

## Generate report in markdown, 
  ## noting how subiterations differ
  ## with persistent graphs
  ##have current version be our baseline
  ##(later) fix HTML report output

###################################
## Training metrics
###################################

## Contrast output when you use acc, costfun, kappa, F1, d' as training metrics

###################################
## TUNING GRID
###################################

## See Code emailed by Brandon 5/23/2018

###################################
## CONVOLUTIONAL NEURAL NETWORK
###################################

## Brandon suggests looking into different Neural network model: convolutional vs recurrent.

###################################
## Ensemble Model: TK Improvement Opportunity
###################################

# To improve ensembling results, try to train an ensemble model that takes predictions as input, instead of imposing 
# an arbitrary rule. 
# see e.g. https://www.analyticsvidhya.com/blog/2017/02/introduction-to-ensembling-along-with-implementation-in-r/

#tk njb should an ensemble model be trained on the training set? (no: risk of over-fitting) On the validation set? 
#(no: what's left to test on?). Train on half the set-aside validation set and test on the other half (yes?).
  
#tk do I include the original predictors as well? So there's more context in which to select one model over another?

##################################################################
## DECIDE WHO TO REFER TO CARE MANAGEMENT / UTILIZATION MANAGEMENT
## In development
##################################################################

##LIFT
#lift <- function(depvar, predvar, groups=10) {
#  if(is.factor(depvar)) depvar<-as.integer(as.character(depvar))
#  if(is.factor(predvar)) predvar<-as.integer(as.character(predvar))
#  dlift<-data.frame(cbind(depvar,predvar))
#  dlift[,"bucket"]=ntile(-dlift[,"predvar"],groups)
#  gaintable=dlift %>% group_by(bucket) %>%
#    summarise_at(vars(depvar),funs(total=n(),totalresp=sum(.,na.rm=T))) %>%
#    mutate(Cumresp=cumsum(totalresp),
#           Gain=Cumresp/sum(totalresp)*100,
#           Cumlift=Gain/(bucket*(100/groups))
#          )
#  return(gaintable)
#}
#
#dlift_lace<-lift(dtrain$followed_by_30d_readmit,dtrain$score_lace)
#
#plot(dlift_lace,main="Lift Chart - LACE Score",
#     x=dlift_lace$bucket,y=dlift_lace$Cumlift,type="I",ylab="Cumulative Lift",xlab="Bucket")
#
## Lift Chart
#pred_lace<-prediction(predictions=dtrain$score_lace,labels=dtrain$followed_by_30d_readmit)
#objlift_lace<-performance(pred_lace,measure="lift",x.measure="rpp")
#plot(objlift_lace,main="Lift Chart - LACE Score",xlab="% Population",ylab="Lift",col="black")
#  abline(1,0,col="grey")
#
## Gain Table
#tbl_gains<-gains(actual=dtrain$is_followed_by_a_30d_readmit,predicted=dtrain$score_lace,groups=10)
#ggplot(tbl_gains, aes(x=tbl_gains[1])),main="Lift Chart - LACE Score",xlab="% Population",ylab="Lift",col="black")

  
  


  

## REPORT

##################################################################
## Group statistics across models
##################################################################
    
# Combine (Training) Accuracy and Kappa from all models into a single data table
  
ak<-as.data.frame(rbind(c("Logistic Regression",ak_lr),
                        c("Decision Tree",ak_dt),
                        c("Random Forest",ak_rf),
                        c("Gradient Boost",ak_gbm),
                        c("Neural Net",ak_nn)))

names(ak)[1]<-"Model"

# Combine Variable Importance Tables from all models into a single data table
  
vi_lr_df<-data.frame(cbind(rownames(vi_lr$importance),round(vi_lr$importance,4)))
  
names(vi_lr_df)<-c("VarName","VI_LR")
    
vi_dt_df<-data.frame(cbind(rownames(vi_dt$importance),round(vi_dt$importance,4)))
  
names(vi_dt_df)<-c("VarName","VI_DT")

vi_rf_df<-data.frame(cbind(rownames(vi_rf$importance),round(vi_rf$importance,4)))
  
names(vi_rf_df)<-c("VarName","VI_RF")

vi_gbm_df<-data.frame(cbind(rownames(vi_gbm$importance),round(vi_gbm$importance,4)))
  
names(vi_gbm_df)<-c("VarName","VI_GBM")

vi_nn_df<-data.frame(cbind(rownames(vi_nn$importance),round(vi_nn$importance,4)))
  
names(vi_nn_df)<-c("VarName","VI_NN")

vi<-merge(merge(merge(vi_lr_df,vi_dt_df),merge(vi_rf_df,vi_gbm_df)),vi_nn_df)
  
vi<-vi[order(-vi$VI_DT),]

# Combine Diagnostic Metrics from all models into a single data table

diag<-as.data.frame(rbind(c("Logistic Regression",diag_lr),
                          c("Decision Tree",diag_dt),
                          c("Random Forest",diag_rf),
                          c("Gradient Boost",diag_gbm),
                          c("Neural Net",diag_nn),
                          c("Ensemble-Majority",diag_esm),
                          c("Ensemble-Black Ball Wins",diag_esbbw),
                          c("Baseline - LACE",diag_lace)))

names(diag)[1]<-"Model"
  
########################
# Output report to DOC  
########################
  
rtffile <- RTF(paste("Readmissions/Docs/Model/Model_1/Readmission_",format(Sys.time(),format="%Y%m%d_%H%M%S"),".doc"),
               font.size=10)
  
addHeader(rtffile,"Readmission Modeling Report",font.size=13, TOC.level=1)
addHeader(rtffile,"Iteration 1: Trained vs. LACE",font.size=13, TOC.level=1)
  addNewLine(rtffile,n=3)
addHeader(rtffile,"Parameters",font.size=11)
  addParagraph(rtffile,paste("Input fields: LACE set", " - ", 
                             "Transformations: Upsampled; LOS ceiling", " - ", 
                             "Training metric: Cost Function", " - ", 
                             "Performance metric: Kappa, AUPRC", " - ", 
                             "This report is generated on ",format(Sys.time(),format="%m/%d/%Y @ %H:%M:%S.")
                            )
              )
  addNewLine(rtffile,n=1)
 
addHeader(rtffile,"Training Sample",font.size=11)
  increaseIndent(rtffile)
  
  addParagraph(rtffile,"Accuracy and Kappa")
  addNewLine(rtffile,n=1)
  addTable(rtffile,ak,col.width=c(1.4,0.8,0.8,0.8))
  addNewLine(rtffile,n=1)
  addParagraph(rtffile,"Variable Importance")
  addNewLine(rtffile,n=1)
  addTable(rtffile,vi,col.width=c(1.8,0.8,0.8,0.8,0.8,0.8))
  addNewLine(rtffile,n=1)
decreaseIndent(rtffile)
addPageBreak(rtffile)

addHeader(rtffile,"Validation Sample",font.size=11)
  increaseIndent(rtffile)
  
  addParagraph(rtffile,"Diagnostic Metrics")
  # (TK OPTION: if cost sum is used, then note:)
  addParagraph(rtffile,paste("Note: Assumes False Negative is ",cost_fn_fp_ratio," times more costly than False Positive."))
  addNewLine(rtffile,n=1)
  addTable(rtffile,diag[,c(1,2,3,4,5,6)],col.width=c(1.25,0.75,0.75,0.75,0.75,0.75))
  addNewLine(rtffile,n=1)
  addPageBreak(rtffile)
  
  addParagraph(rtffile,"Confusion Matrix")
    addNewLine(rtffile,n=1)
    increaseIndent(rtffile)
    addParagraph(rtffile,"CM (Logistic Regression)")
    addTable(rtffile,CM_LR$table,col.width=c(1.0,0.8,0.8))
    addParagraph(rtffile,"CM (Decision Tree)")
    addTable(rtffile,CM_DT$table,col.width=c(1.0,0.8,0.8))
    addParagraph(rtffile,"CM (Random Forest)")
    addTable(rtffile,CM_RF$table,col.width=c(1.0,0.8,0.8))
    addParagraph(rtffile,"CM (Gradient Boost)")
    addTable(rtffile,CM_GBM$table,col.width=c(1.0,0.8,0.8))
    addParagraph(rtffile,"CM (Neural Net)")
    addTable(rtffile,CM_NN$table,col.width=c(1.0,0.8,0.8))
    addParagraph(rtffile,"CM (Ensemble Majority Vote)")
    addTable(rtffile,CM_ESM$table,col.width=c(1.0,0.8,0.8))
    addParagraph(rtffile,"CM (Ensemble Black Ball Wins)")
    addTable(rtffile,CM_ESBBW$table,col.width=c(1.0,0.8,0.8))
    addParagraph(rtffile,"CM (Baseline - LACE)")
    addTable(rtffile,CM_LACE$table,col.width=c(1.0,0.8,0.8))
    addNewLine(rtffile,n=1)
  decreaseIndent(rtffile)
  addPageBreak(rtffile)


  
  addParagraph(rtffile,"AUPRC")
    increaseIndent(rtffile)
    addPng.RTF(rtffile,file = "Readmissions/Docs/Model/Model_1/Graphs/auprc_lr.png", width = 2.6, height = 2.4) 
    addPng.RTF(rtffile,file = "Readmissions/Docs/Model/Model_1/Graphs/auprc_dt.png", width = 2.6, height = 2.4) 
    addPng.RTF(rtffile,file = "Readmissions/Docs/Model/Model_1/Graphs/auprc_rf.png", width = 2.6, height = 2.4) 
    addPng.RTF(rtffile,file = "Readmissions/Docs/Model/Model_1/Graphs/auprc_gbm.png", width = 2.6, height = 2.4) 
    addPng.RTF(rtffile,file = "Readmissions/Docs/Model/Model_1/Graphs/auprc_nn.png", width = 2.6, height = 2.4) 
    addPng.RTF(rtffile,file = "Readmissions/Docs/Model/Model_1/Graphs/auprc_esm.png", width = 2.6, height = 2.4) 
    addPng.RTF(rtffile,file = "Readmissions/Docs/Model/Model_1/Graphs/auprc_esbbw.png", width = 2.6, height = 2.4) 
    addPng.RTF(rtffile,file = "Readmissions/Docs/Model/Model_1/Graphs/auprc_lace.png", width = 2.6, height = 2.4) 
    addNewLine(rtffile,n=1)
  decreaseIndent(rtffile)

done(rtffile)
  
##################################
# Generate report in HTML
# In development, format can be improved. 
# Unclear if this is the right output desired.
##################################    
  
HTMLStart(outdir="/home/cdsw",file=paste("Readmissions/Docs/Model/Model_1/Readmission_",format(Sys.time(),format="%Y%m%d_%H%M%S")),extension="html",echo=F,HTML=T)

HTML.title("Readmission Modeling Report",HR=1)
  
filename=paste("Readmission_",format(Sys.time(),format="%Y%m%d_%H%M%S"))

HTMLStart(outdir="/home/cdsw/Readmissions/Docs/Model/Model_1/",file=filename,extension="html",echo=F,HTML=T)

HTML.title("Readmission Modeling Report",HR=1)

HTMLhr()

HTML.title("Readmission - Training Sample",HR=2)

HTML.title("Readmission - Accuracy and Kappa",HR=3)
ak

HTML.title("Readmission - Variable Importance",HR=3)
vi

HTMLhr()

HTML.title("Readmission - Validation Sample",HR=2)
HTML.title("Readmission - Confusion Matrix",HR=3)
HTML.title("Readmission - Confusion Matrix (Logistic Regression)",HR=4)
CM_LR$table
HTML.title("Readmission - Confusion Matrix (Decision Tree)",HR=4)
CM_DT$table
HTML.title("Readmission - Confusion Matrix (Random Forest)",HR=4)
CM_RF$table
HTML.title("Readmission - Confusion Matrix (LACE)",HR=4)
CM_LACE$table

HTMLhr()

HTML.title("Readmission - Diagnostic Metrics",HR=3)
diag

HTMLhr()

#tk need to add directory?
HTML.title("Readmission - AUROC and AUPRC",HR=3)
HTML.title("Readmission - AUROC and AUPRC (Logistic Regression)",HR=4)
HTMLInsertGraph("auroc_lr.png", Caption="Linear Regression", append=T)
HTMLInsertGraph("auprc_lr.png", Caption="Linear Regression", append=T)
HTML.title("Readmission - AUROC and AUPRC (Decision Tree)",HR=4)
HTMLInsertGraph("auroc_dt.png", Caption="Decision Tree", append=T)
HTMLInsertGraph("auprc_dt.png", Caption="Decision Tree", append=T)
HTML.title("Readmission - AUROC and AUPRC (Random Forest)",HR=4)
HTMLInsertGraph("auroc_rf.png", Caption="Random Forest", append=T)
HTMLInsertGraph("auprc_rf.png", Caption="Random Forest", append=T)
HTML.title("Readmission - AUROC and AUPRC (LACE)",HR=4)
HTMLInsertGraph("auroc_lace.png", Caption="Baseline LACE", append=T)
HTMLInsertGraph("auprc_lace.png", Caption="Baseline LACE", append=T)

HTMLhr()

HTMLStop()

  
  


  

# STOP THE SPARK SESSION

spark_disconnect(spark)
