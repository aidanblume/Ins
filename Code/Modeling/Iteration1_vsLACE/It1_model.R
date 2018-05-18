# Load required packages

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


# TK Report
HTMLStart(outdir="/home/cdsw",file=paste("Readmission_",format(Sys.time(),format="%Y%m%d_%H%M%S")),extension="html",echo=F,HTML=T)
HTML.title("Readmission Modeling Report",HR=1)
  

  
#DATA IMPORT
  
  
  
# WORK OFF OF YARN
  
#set up spark connection
sc_config <- spark_config()
sc_config$spark.kryoserializer.buffer.max <- '1G'
sc <- spark_connect(master = "yarn", config = sc_config)
#sc <- spark_connect(master = "local", config = sc_config)

# Import the **njb_analytic_set** table as a Spark Data Frame
# Make sure column 1 is key - used row_number() in SQL but dropped before export results # njb return to this for understanding/action
  #njb: using dplyer mutate()
sc_model <- spark_read_parquet(sc, name="njb_analytic_set_lace",
        path="hdfs://nameservice1/user/hive/warehouse/nathalie/njb_analytic_set_lace",
        header=T,memory=F)
#sc_model %>% 
#  mutate(cin_no = unlist(lapply(cin_no, function(e) rawToChar(e))))
#  mutate(severity = unlist(lapply(severity, function(e) rawToChar(e))))
#  mutate(aprdrg = unlist(lapply(aprdrg, function(e) rawToChar(e))))
model_raw<-collect(sc_model)

# General notes on this file:
#"sc" stands for spark connect
# don't change lines 33 to 40: these are parameters for setting up a connection. 
#to achieve lazy loading of the data set, on ln 44 (spark_read_parquet()), 
#set memory = F. If memory = T, then loading will be slower. 
#With memory = T, only header is loaded at first; data are loaded as needed.
#in noting the source, drop '.db' from 'nathalie.db'

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
model<-model_raw[which(model_raw$cci != 1),]
  
# Descriptive on model data
paste('Model data has',dim(model)[1],'rows and',dim(model)[2], 'columns.',sep=' ')

# Exploratory Data Analysis - Summary statistics
# Further expanded for all numeric and integer variables below
t(summary(model))

# Exploratory Data Analysis - Summary of Missing
totmiss <- round(sum(is.na(model))/(nrow(model)*ncol(model))*100,digits=2)
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
plot_missing(model,title="Plot of Missing Values")
}

# Categorical variables
# cat_var <- names(model)[which(sapply(model,is.character))]
# if (length(cat_var)>0) {
#  colSums(sapply(model[,cat_var],is.na))
# } else {
#   print("No categorical variable found!")
# }

# Numeric variables
# numeric_var <- names(model)[which(sapply(model,is.numeric))]

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
for (i in 2:dim(model)[2]) {
  if (class(model[[i]])=="numeric" || class(model[[i]])=="integer") {
  sumstat[i-1,1]  <- i
  sumstat[i-1,2]  <- names(model)[i]
  sumstat[i-1,3]  <- class(model[[i]])
  sumstat[i-1,4]  <- sum(is.na(model[[i]]))
  sumstat[i-1,5]  <- min(model[[i]],na.rm=T)
  sumstat[i-1,6]  <- quantile(model[[i]],0.01,na.rm=T)
  sumstat[i-1,7]  <- quantile(model[[i]],0.05,na.rm=T)
  sumstat[i-1,8]  <- quantile(model[[i]],0.10,na.rm=T)
  sumstat[i-1,9]  <- quantile(model[[i]],0.25,na.rm=T)
  sumstat[i-1,10]  <- median(model[[i]],na.rm=T)
  sumstat[i-1,11] <- mean(model[[i]],na.rm=T)
  sumstat[i-1,12] <- quantile(model[[i]],0.75,na.rm=T)
  sumstat[i-1,13] <- quantile(model[[i]],0.90,na.rm=T)
  sumstat[i-1,14] <- quantile(model[[i]],0.95,na.rm=T)
  sumstat[i-1,15] <- quantile(model[[i]],0.99,na.rm=T)
  sumstat[i-1,16] <- max(model[[i]],na.rm=T)
  }
}

# Export descriptive statistics to hive table
sdf_copy_to(sc,sumstat,overwrite=TRUE)
DBI::dbGetQuery(sc,"drop table if exists nathalie.readm_eda_sumstat")
DBI::dbGetQuery(sc,"create table nathalie.readm_eda_sumstat as select * from sumstat")

# Check near-zero of values in every variable
nzvar <- nearZeroVar(model, saveMetrics = TRUE)
nzvar$varn <- row(nzvar)[,1]
nzvar$varnm <- names(model)
# nzvar$exclude <- nzvar$zeroVar | nzvar$nzv
nzvar$ex <- nzvar$percentUnique < 1

# Export near zero check results to hive table
sdf_copy_to(sc,nzvar,overwrite=TRUE)
DBI::dbGetQuery(sc,"drop table if exists nathalie.readm_eda_nzvar")
DBI::dbGetQuery(sc,"create table nathalie.readm_eda_nzvar as select * from nzvar")

# Remove variables without any variance in value
#exclude<-nzvar[nzvar$ex,"varnm"]
# Remove variables not considered for modeling, including key
# Not used for LACE iteration 0 and 1 since no data issue is found
#exclude<-c(exclude,"cin_no")

# Scale final modeling variables for modeling
#final<-scale(model[,!colnames(model) %in% exclude],center=T,scale=T)

#END OF YARN DATA BLOCKS

#########################################################################
# Alternative to Spark import - Import file locally via CSV
# 2017 all data
#model <- read.csv(file="query-impala-25596.csv", header=T, sep=",")
# 2017 exclude CCI
#model <- read.csv(file="query-hive-27290.csv", header=T, sep=",")
#########################################################################

#DATA INVARIANT TRANSFORMATIONS

# Baseline Model - LACE
# Since LACE is a score outside of the modeling process, it can be score here first
# LACE score: Feature engineering that is informed by the literature and not by peeking at the data
# Note: Adding 1 to LOS to align with LACE definition
model$los_lace <- model$los + 1
model$score_a <- model$acuity * 3
model$score_c <- model$previousmyocardialinfarction * 1 + model$cerebrovasculardisease * 1 +
                 model$peripheralvasculardisease * 1 + model$diabeteswithoutcomplications * 1 +
                 model$congestiveheartfailure * 2 + model$diabeteswithendorgandamage * 2 +
                 model$chronicpulmonarydisease * 2 + model$mildliverorrenaldisease * 2 +
                 model$anytumor * 2 +
                 model$dementia * 3 + model$connectivetissuedisease * 3 +
                 model$aids * 4 + model$moderateorsevereliverorrenaldisease * 4 +
                 model$metastaticsolidtumor * 6
model$score_e = ifelse(model$er_visits>4, 4, model$er_visits)
model$score_l = ifelse(model$los_lace <= 3, model$los_lace, 
                       ifelse(model$los_lace <= 6, 4, 
                              ifelse(model$los_lace <= 13, 5, 
                                     ifelse(model$los_lace>=14, 7, 0)
                                    )
                             )
                      )
model$score_lace = model$score_l + model$score_a + model$score_c + model$score_e

#remove from data set (test + train) any row where 'los' or 'acuity' are null because the LACE score for those 
#rows would be artificially depressed.
model <- na.omit(model)

# LACE score summary
model %>% count(score_lace)

# Create new Y as factor
model$V_Target <- factor(model$is_followed_by_a_30d_readmit)

# SPLIT DATA SET & LEAVE ASIDE TEST SET
  
# Random sampling, partition data into training (70%), remaining for validation (30%)
set.seed(1234)
inTrain <- createDataPartition(y=model$is_followed_by_a_30d_readmit, p=0.7, list=F)
dtrain <- model[inTrain,]
dvalid <- model[-inTrain,]

# All samples
model %>% count(is_followed_by_a_30d_readmit)
# Training sample (70%)
dtrain %>% count(is_followed_by_a_30d_readmit)
# Validation sample (30%)
dvalid %>% count(is_followed_by_a_30d_readmit)

# Plot target variable
# All samples
plot(factor(model$is_followed_by_a_30d_readmit))
# Training sample (70%)
plot(factor(dtrain$is_followed_by_a_30d_readmit))
# Validation sample (30%)
plot(factor(dvalid$is_followed_by_a_30d_readmit))
  

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
  

#PLACEHODLER: EXPLORATORY DATA ANALYSIS
  
  # TK nzv etc bring down here
  
  # Missing value imputation here
  
  # Consider class imbalance  
  
#PLACEHOLDER: TRANSFORM VALIDATION DATA
# TK to do after the training transformation processed has been canned and can be applied here. 
# Apply to the test data the data transformation process that was determined with the training set

# Hard coded cost_fn_fp_ratio=10 due to it being in function
# In the future will need to have it refer to cost_fn_fp_ratio directly

# Get cost for false positive and false negative
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
  
  
# MODEL TRAINING

#######################
# Logistic Regression
#######################
# No tuning parameter for glm in caret (ref: https://stackoverflow.com/questions/47822694/logistic-regression-tuning-parameter-grid-in-r-caret-package).
# TK Consider using a different approach to logistic regression for better chance to optimize model

logitReg <- train(V_Target ~ los_lace + acuity + cerebrovasculardisease +
                  peripheralvasculardisease + diabeteswithoutcomplications + congestiveheartfailure +
                  diabeteswithendorgandamage + chronicpulmonarydisease + mildliverorrenaldisease +
                  anytumor + dementia + connectivetissuedisease + aids + metastaticsolidtumor + er_visits,
                  data=dtrain, method="glm", metric="CostSum", maximize=F, family=binomial, trControl=fitControl)

#Accuracy and Kappa on training dataset
ak_lr<-logitReg$results
ak_lr<-round(ak_lr[ak_lr$CostSum == min(ak_lr[,"CostSum"]),c("Accuracy","Kappa","CostSum")],4)
#Variable Importance Table on training dataset
vi_lr<-varImp(logitReg)

# Score on validation sample only
pred_lr <- predict(logitReg,dvalid)
# Confusion Matrix
CM_LR <- confusionMatrix(pred_lr,dvalid$V_Target,positive='1') 
# AUROC
roc_lr <- roc.curve(scores.class0 = as.numeric(as.character(pred_lr[dvalid$V_Target=='1'])), 
                    scores.class1 = as.numeric(as.character(pred_lr[dvalid$V_Target=='0'])), curve=T)
# AUPRC
pr_lr <- pr.curve(scores.class0 = as.numeric(as.character(pred_lr[dvalid$V_Target=='1'])), 
                  scores.class1 = as.numeric(as.character(pred_lr[dvalid$V_Target=='0'])), curve = T)
# Output both AUROC and AUPRC charts to the same JPEG for further reporting
png("auroc_lr.png")
plot(roc_lr,main="AUROC Logistic Regression")
dev.off()
png("auprc_lr.png")
plot(pr_lr,main="AUPRC Logistic Regression")
dev.off()
  
#Validation data: d-prime 
n_hit <- CM_LR$table[2,2]
n_miss <- CM_LR$table[1,2]
n_fa <- CM_LR$table[2,1]
n_cr <- CM_LR$table[1,1]
tmp <- dprime(n_hit, n_miss, n_fa, n_cr) #from neuropsychology package
dprime_lr <- tmp[1]

# Diagnostic Metrics
diag_lr<-round(c(CM_LR$overall[1:2],
                 pr_lr$auc.integral,
                 CM_LR$byClass[c(3,6,7,11)],
                 roc_lr$auc,
                 dprime_lr$dprime,
                round((CM_LR$table["0","1"]*cost_fn_fp_ratio+CM_LR$table["1","0"]*1)/sum(CM_LR$table),4)
                ),4)
names(diag_lr)<-c("Accuracy","Kappa","AUPRC Integral","Precision","Recall","F1","Balanced Accuracy","AUROC","D-Prime","Cost")

#################
# Decision Tree
#################

dTree <- train(V_Target ~ los_lace + acuity + cerebrovasculardisease +
                  peripheralvasculardisease + diabeteswithoutcomplications + congestiveheartfailure +
                  diabeteswithendorgandamage + chronicpulmonarydisease + mildliverorrenaldisease +
                  anytumor + dementia + connectivetissuedisease + aids + metastaticsolidtumor + er_visits,
                  data=dtrain, method="rpart", metric="CostSum", maximize=F, trControl=fitControl)  

#Accuracy and Kappa on training dataset
ak_dt<-dTree$results
ak_dt<-round(ak_dt[ak_dt$CostSum == min(ak_dt[,"CostSum"]),c("Accuracy","Kappa","CostSum")],4)
#Variable Importance Table on training dataset
vi_dt<-varImp(dTree)

# Score on validation sample only
pred_dt <- predict(dTree,dvalid)
# Confusion Matrix
CM_DT <- confusionMatrix(pred_dt,dvalid$V_Target,positive='1') 
# AUROC
roc_dt <- roc.curve(scores.class0 = as.numeric(as.character(pred_dt[dvalid$V_Target=='1'])), 
                    scores.class1 = as.numeric(as.character(pred_dt[dvalid$V_Target=='0'])), curve=T)
# AUPRC
pr_dt <- pr.curve(scores.class0 = as.numeric(as.character(pred_dt[dvalid$V_Target=='1'])), 
                  scores.class1 = as.numeric(as.character(pred_dt[dvalid$V_Target=='0'])), curve = T)
# Output both AUROC and AUPRC charts to the same JPEG for further reporting
png("auroc_dt.png")
plot(roc_dt,main="AUROC Decision Tree")
dev.off()
png("auprc_dt.png")
plot(pr_dt,main="AUPRC Decision Tree")
dev.off()

#Validation data: d-prime 
n_hit <- CM_DT$table[2,2]
n_miss <- CM_DT$table[1,2]
n_fa <- CM_DT$table[2,1]
n_cr <- CM_DT$table[1,1]
tmp <- dprime(n_hit, n_miss, n_fa, n_cr) #from neuropsychology package
dprime_dt <- tmp[1]

# Diagnostic Metrics
diag_dt<-round(c(CM_DT$overall[1:2],
                 pr_dt$auc.integral,
                 CM_DT$byClass[c(3,6,7,11)],
                 roc_dt$auc,
                 dprime_dt$dprime,
                round((CM_DT$table["0","1"]*cost_fn_fp_ratio+CM_DT$table["1","0"]*1)/sum(CM_DT$table),4)
                ),4)
names(diag_dt)<-c("Accuracy","Kappa","AUPRC Integral","Precision","Recall","F1","Balanced Accuracy","AUROC","D-Prime","Cost")

#################
# Random Forest
#################

rForest <- train( V_Target ~ los_lace + acuity + cerebrovasculardisease +
                  peripheralvasculardisease + diabeteswithoutcomplications + congestiveheartfailure +
                  diabeteswithendorgandamage + chronicpulmonarydisease + mildliverorrenaldisease +
                  anytumor + dementia + connectivetissuedisease + aids + metastaticsolidtumor + er_visits,
                 data=dtrain, method="rf", metric="CostSum", maximize=F, ntree=500, 
                 trControl=fitControl)  

#Accuracy and Kappa on training dataset
ak_rf<-rForest$results
ak_rf<-round(ak_rf[ak_rf$CostSum == min(ak_rf[,"CostSum"]),c("Accuracy","Kappa","CostSum")],4)
#names(ak_rf)<-c("Accuracy","Kappa")
#Variable Importance Table on training dataset
vi_rf<-varImp(rForest)

# Score on validation sample only
pred_rf <- predict(rForest,dvalid)
# Confusion Matrix
CM_RF <- confusionMatrix(pred_rf,dvalid$V_Target,positive='1') 
# AUROC
roc_rf <- roc.curve(scores.class0 = as.numeric(as.character(pred_rf[dvalid$V_Target=='1'])), 
                    scores.class1 = as.numeric(as.character(pred_rf[dvalid$V_Target=='0'])), curve=T)
# AUPRC
pr_rf <- pr.curve(scores.class0 = as.numeric(as.character(pred_rf[dvalid$V_Target=='1'])), 
                  scores.class1 = as.numeric(as.character(pred_rf[dvalid$V_Target=='0'])), curve = T)
# Output both AUROC and AUPRC charts to the same JPEG for further reporting
png("auroc_rf.png")
plot(roc_rf,main="AUROC Random Forest")
dev.off()
png("auprc_rf.png")
plot(pr_rf,main="AUPRC Random Forest")
dev.off()

#Validation data: d-prime 
n_hit <- CM_RF$table[2,2]
n_miss <- CM_RF$table[1,2]
n_fa <- CM_RF$table[2,1]
n_cr <- CM_RF$table[1,1]
tmp <- dprime(n_hit, n_miss, n_fa, n_cr) #from neuropsychology package
dprime_rf <- tmp[1]
  
# Diagnostic Metrics
diag_rf<-round(c(CM_RF$overall[1:2],
                 pr_rf$auc.integral,
                 CM_RF$byClass[c(3,6,7,11)],
                 roc_rf$auc,
                 dprime_rf$dprime,
                round((CM_RF$table["0","1"]*cost_fn_fp_ratio+CM_RF$table["1","0"]*1)/sum(CM_RF$table),4)
                ),4)
names(diag_rf)<-c("Accuracy","Kappa","AUPRC Integral","Precision","Recall","F1","Balanced Accuracy","AUROC","D-Prime","Cost")
  
#####################
# Gradient Boosting
#####################

#gbmGrid <- expand.grid(interaction.depth=c(1, 3, 5), n.trees = (0:50)*50,
#                   shrinkage=c(0.01, 0.001),
#                   n.minobsinnode=10)

gbmModel <- train(V_Target ~ los_lace + acuity + cerebrovasculardisease +
                  peripheralvasculardisease + diabeteswithoutcomplications + congestiveheartfailure +
                  diabeteswithendorgandamage + chronicpulmonarydisease + mildliverorrenaldisease +
                  anytumor + dementia + connectivetissuedisease + aids + metastaticsolidtumor + er_visits,
                  data=dtrain, method = "gbm", metric = "CostSum", maximize=F, trControl = fitControl,
                  verbose=FALSE)

#Accuracy and Kappa on training dataset
ak_gbm<-gbmModel$results
ak_gbm<-round(ak_gbm[ak_gbm$CostSum == min(ak_gbm[,"CostSum"]),c("Accuracy","Kappa","CostSum")],4)
#Variable Importance Table on training dataset
vi_gbm<-varImp(gbmModel)

# Score on validation sample only
pred_gbm <- predict(gbmModel,dvalid)
# Confusion Matrix
CM_GBM <- confusionMatrix(pred_gbm,dvalid$V_Target,positive='1')
# AUROC
roc_gbm <- roc.curve(scores.class0 = as.numeric(as.character(pred_gbm[dvalid$V_Target=='1'])), 
                    scores.class1 = as.numeric(as.character(pred_gbm[dvalid$V_Target=='0'])), curve=T)
# AUPRC
pr_gbm <- pr.curve(scores.class0 = as.numeric(as.character(pred_gbm[dvalid$V_Target=='1'])), 
                  scores.class1 = as.numeric(as.character(pred_gbm[dvalid$V_Target=='0'])), curve = T)
# Output both AUROC and AUPRC charts to the same JPEG for further reporting
png("auroc_gbm.png")
plot(roc_gbm,main="AUROC Gradient Boost")
dev.off()
png("auprc_gbm.png")
plot(pr_gbm,main="AUPRC Gradient Boost")
dev.off()

#Validation data: d-prime 
n_hit <- CM_GBM$table[2,2]
n_miss <- CM_GBM$table[1,2]
n_fa <- CM_GBM$table[2,1]
n_cr <- CM_GBM$table[1,1]
tmp <- dprime(n_hit, n_miss, n_fa, n_cr) #from neuropsychology package
dprime_gbm <- tmp[1]

# Diagnostic Metrics
diag_gbm<-round(c(CM_GBM$overall[1:2],
                 pr_gbm$auc.integral,
                 CM_GBM$byClass[c(3,6,7,11)],
                 roc_gbm$auc,
                 dprime_gbm$dprime,
                round((CM_GBM$table["0","1"]*cost_fn_fp_ratio+CM_GBM$table["1","0"]*1)/sum(CM_GBM$table),4)
                ),4)
names(diag_gbm)<-c("Accuracy","Kappa","AUPRC Integral","Precision","Recall","F1","Balanced Accuracy","AUROC","D-Prime","Cost")
  
##################
# Neural Network
##################

nnetModel <- train(V_Target ~ los_lace + acuity + cerebrovasculardisease +
                  peripheralvasculardisease + diabeteswithoutcomplications + congestiveheartfailure +
                  diabeteswithendorgandamage + chronicpulmonarydisease + mildliverorrenaldisease +
                  anytumor + dementia + connectivetissuedisease + aids + metastaticsolidtumor + er_visits,
                  data=dtrain, method="nnet", metric = "CostSum", maximize=F, trControl = fitControl)
  
#Accuracy and Kappa on training dataset
ak_nn<-nnetModel$results
ak_nn<-round(ak_nn[ak_nn$CostSum == min(ak_nn[,"CostSum"]),c("Accuracy","Kappa","CostSum")],4)
#Variable Importance Table on training dataset
vi_nn<-varImp(nnetModel)

# Score on validation sample only
pred_nn <- predict(nnetModel,dvalid)
# Confusion Matrix
CM_NN <- confusionMatrix(pred_nn,dvalid$V_Target,positive='1') 
# AUROC
roc_nn <- roc.curve(scores.class0 = as.numeric(as.character(pred_nn[dvalid$V_Target=='1'])), 
                    scores.class1 = as.numeric(as.character(pred_nn[dvalid$V_Target=='0'])), curve=T)
# AUPRC
pr_nn <- pr.curve(scores.class0 = as.numeric(as.character(pred_nn[dvalid$V_Target=='1'])), 
                  scores.class1 = as.numeric(as.character(pred_nn[dvalid$V_Target=='0'])), curve = T)
# Output both AUROC and AUPRC charts to the same JPEG for further reporting
png("auroc_nn.png")
plot(roc_nn,main="AUROC Neural Net")
dev.off()
png("auprc_nn.png")
plot(pr_nn,main="AUPRC Neural Net")
dev.off()

#Validation data: d-prime 
n_hit <- CM_NN$table[2,2]
n_miss <- CM_NN$table[1,2]
n_fa <- CM_NN$table[2,1]
n_cr <- CM_NN$table[1,1]
tmp <- dprime(n_hit, n_miss, n_fa, n_cr) #from neuropsychology package
dprime_nn <- tmp[1]

# Diagnostic Metrics
diag_nn<-round(c(CM_NN$overall[1:2],
                 pr_nn$auc.integral,
                 CM_NN$byClass[c(3,6,7,11)],
                 roc_nn$auc,
                 dprime_nn$dprime,
                round((CM_NN$table["0","1"]*cost_fn_fp_ratio+CM_NN$table["1","0"]*1)/sum(CM_NN$table),4)
                ),4)
names(diag_nn)<-c("Accuracy","Kappa","AUPRC Integral","Precision","Recall","F1","Balanced Accuracy","AUROC","D-Prime","Cost")

#################################
# Ensemble Model: Majority Vote
#################################

#When applying ensemble logic to test set, copy-paste-edit the command below so that it refers to test, not training, predictions.
#To improve, model the outcome of ensemble, see e.g. https://www.analyticsvidhya.com/blog/2017/02/introduction-to-ensembling-along-with-implementation-in-r/

#Generate votes
tmp_df <- data.frame(pred_lr, pred_dt, pred_rf, pred_gbm, pred_nn)
tmp_df2 <- mutate_all(tmp_df, function(x) as.numeric(as.character(x)))
#start validity check: with our data type issues, Qing observed elsewhere that +1 was added to as.numeric(as.character transformation)  
test_rows <- which(pred_lr == 1)
unique(tmp_df[test_rows, ] == tmp_df2[test_rows, ])
#end validity check: all rows are TRUEx4. Transformation is correct; +1 is not added on top
tmp_df2$sum <- tmp_df2$pred_lr + tmp_df2$pred_dt +tmp_df2$pred_rf + tmp_df2$pred_gbm + tmp_df2$pred_nn

#Score validation set based on majority rule (3 out of 5)
pred_esm <- ifelse(tmp_df2$sum >= 3, 1, 0)

# Confusion Matrix
CM_ESM <- confusionMatrix(as.factor(pred_esm),dvalid$V_Target,positive='1')
# AUROC
roc_esm <- roc.curve(scores.class0 = as.numeric(as.character(pred_esm[dvalid$V_Target=='1'])), 
                    scores.class1 = as.numeric(as.character(pred_esm[dvalid$V_Target=='0'])), curve=T)
# AUPRC
pr_esm <- pr.curve(scores.class0 = as.numeric(as.character(pred_esm[dvalid$V_Target=='1'])), 
                  scores.class1 = as.numeric(as.character(pred_esm[dvalid$V_Target=='0'])), curve = T)
# Output both AUROC and AUPRC charts to the same JPEG for further reporting
png("auroc_esm.png")
plot(roc_esm,main="AUROC Ensamble Majority Vote")
dev.off()
png("auprc_esm.png")
plot(pr_esm,main="AUPRC Ensamble Majority Vote")
dev.off()

#Validation data: d-prime 
n_hit <- CM_ESM$table[2,2]
n_miss <- CM_ESM$table[1,2]
n_fa <- CM_ESM$table[2,1]
n_cr <- CM_ESM$table[1,1]
tmp <- dprime(n_hit, n_miss, n_fa, n_cr) #from neuropsychology package
dprime_esm <- tmp[1]

# Diagnostic Metrics
diag_esm<-round(c(CM_ESM$overall[1:2],
                 pr_esm$auc.integral,
                 CM_ESM$byClass[c(3,6,7,11)],
                 roc_esm$auc,
                 dprime_esm$dprime,
                round((CM_ESM$table["0","1"]*cost_fn_fp_ratio+CM_ESM$table["1","0"]*1)/sum(CM_ESM$table),4)
                ),4)
names(diag_esm)<-c("Accuracy","Kappa","AUPRC Integral","Precision","Recall","F1","Balanced Accuracy","AUROC","D-Prime","Cost")

#Score validation set based on maximum prediction rule (>0)
pred_esp <- ifelse(tmp_df2$sum > 0, 1, 0)
# Confusion Matrix
CM_ESP <- confusionMatrix(as.factor(pred_esp),dvalid$V_Target,positive='1')
# AUROC
roc_esp <- roc.curve(scores.class0 = as.numeric(as.character(pred_esp[dvalid$V_Target=='1'])), 
                    scores.class1 = as.numeric(as.character(pred_esp[dvalid$V_Target=='0'])), curve=T)
# AUPRC
pr_esp <- pr.curve(scores.class0 = as.numeric(as.character(pred_esp[dvalid$V_Target=='1'])), 
                  scores.class1 = as.numeric(as.character(pred_esp[dvalid$V_Target=='0'])), curve = T)
# Output both AUROC and AUPRC charts to the same JPEG for further reporting
png("auroc_esp.png")
plot(roc_esp,main="AUROC Ensamble Max Prediction")
dev.off()
png("auprc_esp.png")
plot(pr_esp,main="AUPRC Ensamble Max Prediction")
dev.off()

#Validation data: d-prime 
n_hit <- CM_ESP$table[2,2]
n_miss <- CM_ESP$table[1,2]
n_fa <- CM_ESP$table[2,1]
n_cr <- CM_ESP$table[1,1]
tmp <- dprime(n_hit, n_miss, n_fa, n_cr) #from neuropsychology package
dprime_esp <- tmp[1]

# Diagnostic Metrics
diag_esp<-round(c(CM_ESP$overall[1:2],
                 pr_esp$auc.integral,
                 CM_ESP$byClass[c(3,6,7,11)],
                 roc_esp$auc,
                 dprime_esp$dprime,
                round((CM_ESP$table["0","1"]*cost_fn_fp_ratio+CM_ESP$table["1","0"]*1)/sum(CM_ESP$table),4)
                ),4)
names(diag_esp)<-c("Accuracy","Kappa","AUPRC Integral","Precision","Recall","F1","Balanced Accuracy","AUROC","D-Prime","Cost")
 
# BASELINE MODEL PERFORMANCE (VALIDATION): LACE
# Generate predictions based on LACE scores 
#Use criterion = 10 (in LACE methodology, low+moderate risk vs. high risk). 
#ALT: Assign 0, 1 values to cases by descreasing LACE-score order, where top x% get 1, otherwise get 0; 
#x is determined by the proportion of 1s in the trained model predictions.
pred_lace<-(dvalid$score_lace>=10)*1

#Validation data: Confusion Matrix
CM_LACE <- confusionMatrix(as.factor(pred_lace),dvalid$V_Target,positive='1') 
# AUROC
roc_lace <- roc.curve(scores.class0 = as.numeric(as.character(pred_lace[dvalid$V_Target=='1'])), 
                    scores.class1 = as.numeric(as.character(pred_lace[dvalid$V_Target=='0'])), curve=T)
# AUPRC
pr_lace <- pr.curve(scores.class0 = as.numeric(as.character(pred_lace[dvalid$V_Target=='1'])), 
                  scores.class1 = as.numeric(as.character(pred_lace[dvalid$V_Target=='0'])), curve = T)
# Output both AUROC and AUPRC charts to the same JPEG for further reporting
png("auroc_lace.png")
plot(roc_lace,main="AUROC Baseline - LACE")
dev.off()
png("auprc_lace.png")
plot(pr_lace,main="AUPRC Baseline - LACE")
dev.off()

#Validation data: d-prime 
n_hit <- CM_LACE$table[2,2]
n_miss <- CM_LACE$table[1,2]
n_fa <- CM_LACE$table[2,1]
n_cr <- CM_LACE$table[1,1]
tmp <- dprime(n_hit, n_miss, n_fa, n_cr) #from neuropsychology package
dprime_lace <- tmp[1]

# Diagnostic Metrics
diag_lace<-round(c(CM_LACE$overall[1:2],
                 pr_lace$auc.integral,
                 CM_LACE$byClass[c(3,6,7,11)],
                 roc_lace$auc,
                 dprime_lace$dprime,
                round((CM_LACE$table["0","1"]*cost_fn_fp_ratio+CM_LACE$table["1","0"]*1)/sum(CM_LACE$table),4)
                ),4)
names(diag_lace)<-c("Accuracy","Kappa","AUPRC Integral","Precision","Recall","F1","Balanced Accuracy","AUROC","D-Prime","Cost")


# DECIDE WHO TO REFER TO CARE MANAGEMENT / UTILIZATION MANAGEMENT
  
#LIFT
lift <- function(depvar, predvar, groups=10) {
  if(is.factor(depvar)) depvar<-as.integer(as.character(depvar))
  if(is.factor(predvar)) predvar<-as.integer(as.character(predvar))
  dlift<-data.frame(cbind(depvar,predvar))
  dlift[,"bucket"]=ntile(-dlift[,"predvar"],groups)
  gaintable=dlift %>% group_by(bucket) %>%
    summarise_at(vars(depvar),funs(total=n(),totalresp=sum(.,na.rm=T))) %>%
    mutate(Cumresp=cumsum(totalresp),
           Gain=Cumresp/sum(totalresp)*100,
           Cumlift=Gain/(bucket*(100/groups))
          )
  return(gaintable)
}

dlift_lace<-lift(dtrain$followed_by_30d_readmit,dtrain$score_lace)

plot(dlift_lace,main="Lift Chart - LACE Score",
     x=dlift_lace$bucket,y=dlift_lace$Cumlift,type="I",ylab="Cumulative Lift",xlab="Bucket")

# Lift Chart
pred_lace<-prediction(predictions=dtrain$score_lace,labels=dtrain$followed_by_30d_readmit)
objlift_lace<-performance(pred_lace,measure="lift",x.measure="rpp")
plot(objlift_lace,main="Lift Chart - LACE Score",xlab="% Population",ylab="Lift",col="black")
  abline(1,0,col="grey")

    # Gain Table
tbl_gains<-gains(actual=dtrain$is_followed_by_a_30d_readmit,predicted=dtrain$score_lace,groups=10)
ggplot(tbl_gains, aes(x=tbl_gains[1]))
  ,main="Lift Chart - LACE Score",xlab="% Population",ylab="Lift",col="black")
    
# Print out a report contrasting the models
# Combine Accuracy and Kappa from all models into a single data table
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
                          c("Ensamble-Majority",diag_esm),
                          c("Ensample-MaxPred",diag_esp),
                          c("Baseline - LACE",diag_lace)))
names(diag)[1]<-"Model"

# Generate report in HTML
filename=paste("Readmission_",format(Sys.time(),format="%Y%m%d_%H%M%S"))
HTMLStart(outdir="/home/cdsw",file=filename,extension="html",echo=F,HTML=T)

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


# Output report to DOC  
rtffile <- RTF(paste("Readmission_",format(Sys.time(),format="%Y%m%d_%H%M%S"),".doc"),
               font.size=10)
addHeader(rtffile,"Readmission Modeling Report",font.size=12,TOC.level=1)
addHeader(rtffile,paste("This report is generated on ",format(Sys.time(),format="%m/%d/%Y @ %H:%M:%S.")))
addHeader(rtffile,"Training Sample",font.size=11)
  increaseIndent(rtffile)
  addParagraph(rtffile,"Accuracy and Kappa")
  addNewLine(rtffile,n=1)
  addTable(rtffile,ak,col.width=c(1.4,0.8,0.8,0.8))
  addNewLine(rtffile,n=1)
  addParagraph(rtffile,"Variable Importance")
  addTable(rtffile,vi,col.width=c(1.8,0.8,0.8,0.8,0.8,0.8))
  addNewLine(rtffile,n=1)
decreaseIndent(rtffile)
addHeader(rtffile,"Validation Sample",font.size=11)
  increaseIndent(rtffile)
  addParagraph(rtffile,"Confusion Matrix")
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
    addParagraph(rtffile,"CM (Ensamble Majority Vote)")
    addTable(rtffile,CM_ESM$table,col.width=c(1.0,0.8,0.8))
    addParagraph(rtffile,"CM (Ensamble Maximum Prediction)")
    addTable(rtffile,CM_ESP$table,col.width=c(1.0,0.8,0.8))
    addParagraph(rtffile,"CM (Baseline - LACE)")
    addTable(rtffile,CM_LACE$table,col.width=c(1.0,0.8,0.8))
    addNewLine(rtffile,n=1)
  decreaseIndent(rtffile)
  addPageBreak(rtffile)
  addParagraph(rtffile,"Diagnostic Metrics")
  addParagraph(rtffile,paste(" Note: Assumes False Negative is ",cost_fn_fp_ratio," times more costly than False Positive."))
  decreaseIndent(rtffile)
  addTable(rtffile,diag[,c(1,2,3,4,5,6,9,10,11)],col.width=c(1.25,0.75,0.55,0.65,0.70,0.55,0.65,0.65,0.55))
  increaseIndent(rtffile)
  addNewLine(rtffile,n=1)
  addParagraph(rtffile,"AUROC and AUPRC")
    increaseIndent(rtffile)
    addPng.RTF(rtffile,file = "auroc_lr.png", width = 2.6, height = 2.4) 
    addPng.RTF(rtffile,file = "auprc_lr.png", width = 2.6, height = 2.4) 
    addPng.RTF(rtffile,file = "auroc_dt.png", width = 2.6, height = 2.4) 
    addPng.RTF(rtffile,file = "auprc_dt.png", width = 2.6, height = 2.4) 
    addPng.RTF(rtffile,file = "auroc_rf.png", width = 2.6, height = 2.4) 
    addPng.RTF(rtffile,file = "auprc_rf.png", width = 2.6, height = 2.4) 
    addPng.RTF(rtffile,file = "auroc_gbm.png", width = 2.6, height = 2.4) 
    addPng.RTF(rtffile,file = "auprc_gbm.png", width = 2.6, height = 2.4) 
    addPng.RTF(rtffile,file = "auroc_nn.png", width = 2.6, height = 2.4) 
    addPng.RTF(rtffile,file = "auprc_nn.png", width = 2.6, height = 2.4) 
    addPng.RTF(rtffile,file = "auroc_esm.png", width = 2.6, height = 2.4) 
    addPng.RTF(rtffile,file = "auprc_esm.png", width = 2.6, height = 2.4) 
    addPng.RTF(rtffile,file = "auroc_esp.png", width = 2.6, height = 2.4) 
    addPng.RTF(rtffile,file = "auprc_esp.png", width = 2.6, height = 2.4) 
    addPng.RTF(rtffile,file = "auroc_lace.png", width = 2.6, height = 2.4) 
    addPng.RTF(rtffile,file = "auprc_lace.png", width = 2.6, height = 2.4) 
    addNewLine(rtffile,n=1)
  decreaseIndent(rtffile)
done(rtffile)
