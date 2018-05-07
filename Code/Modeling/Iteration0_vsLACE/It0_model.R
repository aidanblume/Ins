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
              "neuropsychology")

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
model<-collect(sc_model)

# General notes on this file:
#"sc" stands for spark connect
# don't change lines 33 to 40: these are parameters for setting up a connection. 
#to achieve lazy loading of the data set, on ln 44 (spark_read_parquet()), 
#set memory = F. If memory = T, then loading will be slower. 
#With memory = T, only header is loaded at first; data are loaded as needed.
#in noting the source, drop '.db' from 'nathalie.db'

# Fix fields imported as raw from spark_read_parquet
# Put the column numbers for columns needing fix into the 
var_to_fix <- c(1)
fixed <- vector(mode="character",length=length(model$cin_no))
for (j in var_to_fix) {
  for (i in 1:length(model$cin_no)) {
    fixed[i] <- rawToChar(model$cin_no[[i]])
  }
  model[,j]<-fixed
}
  
model1<-data.frame(matrix(unlist(model),nrow=nrow(model),byrow=T))
model2<-data.frame(t(sapply(model,c)))
model3<-dcast(melt(model),L1~L2)


lapply(model, `[[`, 2) %>% 
    data.frame %>% 
    add_rownames("key") %>% 
    gather(x, value, -key) %>% 
    select(-x)

  
# Descriptive on model data
paste('Model data has',dim(model)[1],'rows and',dim(model)[2], 'columns.',sep=' ')

# Exploratory Data Analysis - Summary statistics
summary(model)

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
  if (class(model[[i]]) == "numeric") {
  sumstat[i-1,1]  <- i
  sumstat[i-1,2]  <- names(model)[i]
  sumstat[i-1,3]  <- sum(is.na(model[[i]]))
  sumstat[i-1,4]  <- min(model[[i]],na.rm=T)
  sumstat[i-1,5]  <- quantile(model[[i]],0.01,na.rm=T)
  sumstat[i-1,6]  <- quantile(model[[i]],0.05,na.rm=T)
  sumstat[i-1,7]  <- quantile(model[[i]],0.10,na.rm=T)
  sumstat[i-1,8]  <- quantile(model[[i]],0.25,na.rm=T)
  sumstat[i-1,9]  <- median(model[[i]],na.rm=T)
  sumstat[i-1,10] <- mean(model[[i]],na.rm=T)
  sumstat[i-1,11] <- quantile(model[[i]],0.75,na.rm=T)
  sumstat[i-1,12] <- quantile(model[[i]],0.90,na.rm=T)
  sumstat[i-1,13] <- quantile(model[[i]],0.95,na.rm=T)
  sumstat[i-1,14] <- quantile(model[[i]],0.99,na.rm=T)
  sumstat[i-1,15] <- max(model[[i]],na.rm=T)
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
DBI::dbGetQuery(sc,"drop table if exists poc.poc_eda_nzvar")
DBI::dbGetQuery(sc,"create table poc.poc_eda_nzvar as select * from nzvar")

# Remove variables without any variance in value
exclude<-nzvar[nzvar$ex,"varnm"]
# Remove variables not considered for clustering, including key
exclude<-c(exclude,"c_billedamount","c_allowedamt","c_paidamount","amountpaid","key","provid","claimlines")

# Scale final modeling variables for modeling
final<-scale(model[,!colnames(model) %in% exclude],center=T,scale=T)

  
  #END OF YARN DATA BLOCKS
 
  
  
  
  
# Import file locally via CSV
model <- read.csv(file="/home/cdsw/query-impala-25596.csv", header=T, sep=",")

# Create new Y as factor 
model$V_Target <- factor(model$is_followed_by_a_30d_readmit)
  
  
  
# SPLIT DATA SET & LEAVE ASIDE TEST SET
  
# Random sampling, partition data into training (70%), remaining for validation (30%)
inTrain <- createDataPartition(y=model$is_followed_by_a_30d_readmit, p=0.7, list=F)
dtrain <- model[inTrain,]
dvalid <- model[-inTrain,]

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
  # Plot target variablelot(factor(model$is_followed_by_a_30d_readmit))
model %>% count(is_followed_by_a_30d_readmit)
  

  
#PLACEHOLDER: TRANSFORM VALIDATION DATA
# TK to do after the training transformation processed has been canned and can be applied here. 
# Apply to the test data the data transformation process that was determined with the training set

  
  
# MODEL TRAINING

# Logistic Regression
  # No tuning parameter for glm in caret (ref: https://stackoverflow.com/questions/47822694/logistic-regression-tuning-parameter-grid-in-r-caret-package).
  # TK Consider using a different approach to logistic regression for better chance to optimize model

  #Train model
fitControl <- trainControl(method="cv", number=5, savePrediction=T)
logitReg <- train(V_Target ~ los + acuity + cerebrovasculardisease +
                  peripheralvasculardisease + diabeteswithoutcomplications + congestiveheartfailure +
                  diabeteswithendorgandamage + chronicpulmonarydisease + mildliverorrenaldisease +
                  anytumor + dementia + connectivetissuedisease + aids + metastaticsolidtumor + er_visits,
                  data=dtrain, method="glm", family=binomial(), trControl=fitControl)

  #Placeholder: If tuning, select optimal params here. Show convergence. 
  
  #Variable Importance Table
vi_lr<-varImp(logitReg)
  
  #Score validation set
pred_lr <- predict(logitReg,dvalid)

  #Training data: Accuracy and Kappa
ak_lr<-round(c(logitReg$results[1,2],logitReg$results[1,3]),4)
names(ak_lr)<-c("Accuracy","Kappa")

  #Validation data: Confusion Matrix
CM_LR <- confusionMatrix(pred_lr,dvalid$V_Target,positive='1') 

  #Validation data: d-prime 
n_hit <- CM_LR$table[2,2]
n_miss <- CM_LR$table[1,2]
n_fa <- CM_LR$table[2,1]
n_cr <- CM_LR$table[1,1]
tmp <- dprime(n_hit, n_miss, n_fa, n_cr) #from neuropsychology package
dprime_lr <- tmp[1]
  
  #COllect together Diagnostic Metrics (Accuracy, Kappa, Pos Pred Value, Recall, F1, Balanced Accuracy/AUROC, d')
diag_lr<-round(c(CM_LR$overall[1:2],CM_LR$byClass[c(3,6,7,11)],dprime=as.numeric(dprime_lr)),4)

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
  
  
  
  
# TK Analysis of Performance metrics (with an eye toward detecting class imbalance)
# AUROC vs PR-AUC; Kappa; d'; Kolmogorov-Smirnov; F1   
  

  
  

# Decision Tree
dTree <- train(V_Target ~ los + acuity + cerebrovasculardisease +
                  peripheralvasculardisease + diabeteswithoutcomplications + congestiveheartfailure +
                  diabeteswithendorgandamage + chronicpulmonarydisease + mildliverorrenaldisease +
                  anytumor + dementia + connectivetissuedisease + aids + metastaticsolidtumor + er_visits,
                  data=dtrain, method="ctree", trControl=fitControl)  


  
# Random Forest
#levels(dtrain$followed_by_30d_readmit) <- make.names(levels(factor(train$is_followed_by_a_30d_readmit)))
rForest <- train( V_Target ~ los + acuity + cerebrovasculardisease +
                  peripheralvasculardisease + diabeteswithoutcomplications + congestiveheartfailure +
                  diabeteswithendorgandamage + chronicpulmonarydisease + mildliverorrenaldisease +
                  anytumor + dementia + connectivetissuedisease + aids + metastaticsolidtumor + er_visits,
                 data=dtrain, 
                 method="rf", 
                 ntree=500, 
                 trControl=trainControl(method="cv", number=3))  
  
                #vi_rf<-varImp(rForest)
                #  
                #  #Score validation set
                #pred_rf <- predict(rForest,dvalid)
                #
                #  #Training data: Accuracy and Kappa
                #ak_rf<-round(c(rForest$results[1,2],rForest$results[1,3]),4)
                #names(ak_rf)<-c("Accuracy","Kappa")
                #
                #  #Validation data: Confusion Matrix
                #CM_rf <- confusionMatrix(pred_rf,dvalid$V_Target,positive='1') 
                #
                #  #Validation data: d-prime 
                #n_hit <- CM_rf$table[2,2]
                #n_miss <- CM_rf$table[1,2]
                #n_fa <- CM_rf$table[2,1]
                #n_cr <- CM_rf$table[1,1]
                #tmp <- dprime(n_hit, n_miss, n_fa, n_cr) #from neuropsychology package
                #dprime_rf <- tmp[1]  
                #  #Collect together Diagnostic Metrics (Accuracy, Kappa, Pos Pred Value, Recall, F1, Balanced Accuracy/AUROC, d')
                #diag_rf<-round(c(CM_rf$overall[1:2],CM_rf$byClass[c(3,6,7,11)],dprime=as.numeric(dprime_rf)),4)
  
  
# Gradient Boosting
  
#gbmGrid <- expand.grid(interaction.depth=c(1, 3, 5), n.trees = (0:50)*50,
#                   shrinkage=c(0.01, 0.001),
#                   n.minobsinnode=10)

fitControl <- trainControl(method="cv", number=5)

gbmModel <- train(V_Target ~ los + acuity + cerebrovasculardisease +
                  peripheralvasculardisease + diabeteswithoutcomplications + congestiveheartfailure +
                  diabeteswithendorgandamage + chronicpulmonarydisease + mildliverorrenaldisease +
                  anytumor + dementia + connectivetissuedisease + aids + metastaticsolidtumor + er_visits,
                  data=dtrain,
                  method = "gbm",
                  metric = "Accuracy",
                  trControl = fitControl,
                  verbose=FALSE)

                  #vi_gbm<-varImp(gbmModel)
                  #
                  #  #Score validation set
                  #pred_gbm <- predict(gbmModel,dvalid)
                  #
                  #  #Training data: Accuracy and Kappa
                  #ak_gbm<-round(c(gbmModel$results[1,2],gbmModel$results[1,3]),4)
                  #names(ak_gbm)<-c("Accuracy","Kappa")
                  #
                  #  #Validation data: Confusion Matrix
                  #CM_gbm <- confusionMatrix(pred_gbm,dvalid$V_Target,positive='1') 
                  #
                  #  #Validation data: d-prime 
                  #n_hit <- CM_gbm$table[2,2]
                  #n_miss <- CM_gbm$table[1,2]
                  #n_fa <- CM_gbm$table[2,1]
                  #n_cr <- CM_gbm$table[1,1]
                  #tmp <- dprime(n_hit, n_miss, n_fa, n_cr) #from neuropsychology package
                  #dprime_gbm <- tmp[1]
                  #  #Collect together Diagnostic Metrics (Accuracy, Kappa, Pos Pred Value, Recall, F1, Balanced Accuracy/AUROC, d')
                  #diag_gbm<-round(c(CM_gbm$overall[1:2],CM_gbm$byClass[c(3,6,7,11)],dprime=as.numeric(dprime_gbm)),4)



# Neural Network

fitControl <- trainControl(method="cv", number=5)
nnetModel <- train(V_Target ~ los + acuity + cerebrovasculardisease +
                  peripheralvasculardisease + diabeteswithoutcomplications + congestiveheartfailure +
                  diabeteswithendorgandamage + chronicpulmonarydisease + mildliverorrenaldisease +
                  anytumor + dementia + connectivetissuedisease + aids + metastaticsolidtumor + er_visits,
                  data=dtrain, 
                  method="nnet", 
                  trControl = fitControl)
  
                  #vi_nnet<-varImp(nnetModel)
                  #
                  #  #Score validation set
                  #pred_nnet <- predict(nnetModel,dvalid)
                  #
                  #  #Training data: Accuracy and Kappa
                  #ak_nnet<-round(c(nnetModel$results[1,2],nnetModel$results[1,3]),4)
                  #names(ak_nnet)<-c("Accuracy","Kappa")
                  #
                  #  #Validation data: Confusion Matrix
                  #CM_nnet <- confusionMatrix(pred_nnet,dvalid$V_Target,positive='1') 
                  #
                  #  #Validation data: d-prime 
                  #n_hit <- CM_nnet$table[2,2]
                  #n_miss <- CM_nnet$table[1,2]
                  #n_fa <- CM_nnet$table[2,1]
                  #n_cr <- CM_nnet$table[1,1]
                  #tmp <- dprime(n_hit, n_miss, n_fa, n_cr) #from neuropsychology package
                  #dprime_nnet <- tmp[1]
                  #  #Collect together Diagnostic Metrics (Accuracy, Kappa, Pos Pred Value, Recall, F1, Balanced Accuracy/AUROC, d')
                  #diag_nnet<-round(c(CM_nnet$overall[1:2],CM_nnet$byClass[c(3,6,7,11)],dprime=as.numeric(dprime_nnet)),4)


# Ensemble Model: Majority Vote
#When applying ensemble logic to test set, copy-paste-edit the command below so that it refers to test, not training, predictions.
#To improve, model the outcome of ensemble, see e.g. https://www.analyticsvidhya.com/blog/2017/02/introduction-to-ensembling-along-with-implementation-in-r/

  #Generate votes
pred_lr <- predict(logitReg,dvalid)
pred_rf <- predict(rForest,dvalid)
pred_gbm <- predict(gbmModel,dvalid)
pred_nnet <- predict(nnetModel,dvalid)
tmp_df <- data.frame(pred_lr, pred_rf, pred_gbm, pred_nnet)
tmp_df2 <- mutate_all(tmp_df, function(x) as.numeric(as.character(x)))
  #start validity check: with our data type issues, Qing observed elsehwre that +1 was added to as.numeric(as.character transformation)  
test_rows <- which(pred_lr == 1)
unique(tmp_df[test_rows, ] == tmp_df2[test_rows, ])
  #end validity check: all rows are TRUEx4. Transformation is correct; +1 is not added on top
tmp_df2$sum <- tmp_df2$pred_lr + tmp_df2$pred_rf + tmp_df2$pred_gbm + tmp_df2$pred_nnet

  #Score validation set
pred_ensmaj <- ifelse(tmp_df2$sum >= 3, 1, 0)                                                         

  #Performance
                #  #Validation data: Confusion Matrix
                #CM_ensmaj <- confusionMatrix(as.factor(pred_ensmaj),dvalid$V_Target,positive='1') 
                #
                #  #Validation data: d-prime 
                #n_hit <- CM_ensmaj$table[2,2]
                #n_miss <- CM_ensmaj$table[1,2]
                #n_fa <- CM_ensmaj$table[2,1]
                #n_cr <- CM_ensmaj$table[1,1]
                #tmp <- dprime(n_hit, n_miss, n_fa, n_cr) #from neuropsychology package
                #dprime_ensmaj <- tmp[1]
                #  #Collect together Diagnostic Metrics (Accuracy, Kappa, Pos Pred Value, Recall, F1, Balanced Accuracy/AUROC, d')
                #diag_ensmaj<-round(c(CM_ensmaj$overall[1:2],CM_ensmaj$byClass[c(3,6,7,11)],dprime=as.numeric(dprime_ensmaj)),4)

  
# BASELINE MODEL PERFORMANCE (VALIDATION): LACE

  # Generate LACE Scores for the validation set  #TK: Decide whether or not to impute missing values. Now: cases with missing values are omited  
dvalid$los2 <- dvalid$los + 1  #Adding 1 to LOS to align with LACE definition.
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
dvalid$score_l = ifelse(dvalid$los2 <= 3, dvalid$los2, 
                       ifelse(dvalid$los2 <= 6, 4, 
                              ifelse(dvalid$los2 <= 13, 5, 
                                     ifelse(dvalid$los2>=14, 7, 0)
                                    )
                             )
                      )
dvalid$score_lace = dvalid$score_l + dvalid$score_a + dvalid$score_c + dvalid$score_e
  # remove from data set (test + train) any row where 'los' or 'acuity' are null because the LACE score for those rows would be artificially depressed.
nrow(dvalid)
dvalid2 <- na.omit(dvalid)
nrow(dvalid2)
  
  # Generate predictions based on LACE scores 
    #Use criterion = 10 (in LACE methodology, low+moderate risk vs. high risk). 
    #ALT: Assign 0, 1 values to cases by descreasing LACE-score order, where top x% get 1, otherwise get 0; 
    #x is determined by the proportion of 1s in the trained model predictions.
pred_lace<-(dvalid2$score_lace>=10)*1

  #Validation data: Confusion Matrix
CM_lace <- confusionMatrix(as.factor(pred_lace),dvalid$V_Target,positive='1') 
CM_lace$table
  
  #Validation data: d-prime 
n_hit <- CM_lace$table[2,2]
n_miss <- CM_lace$table[1,2]
n_fa <- CM_lace$table[2,1]
n_cr <- CM_lace$table[1,1]
tmp <- dprime(n_hit, n_miss, n_fa, n_cr) #from neuropsychology package
dprime_lace <- tmp[1]
  #Collect together Diagnostic Metrics (Accuracy, Kappa, Pos Pred Value, Recall, F1, Balanced Accuracy/AUROC, d')
diag_lace<-round(c(CM_lace$overall[1:2],CM_lace$byClass[c(3,6,7,11)],dprime=as.numeric(dprime_lace)),4)

  
  

  
  
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
                        c("Random Forest",ak_rf)))
names(ak)[1]<-"Model"

# Combine Variable Importance Tables from all models into a single data table
vi_lr_df<-data.frame(cbind(rownames(vi_lr$importance),round(vi_lr$importance,4)))
names(vi_lr_df)<-c("VarName","VI_LR")
    
vi_dt_df<-data.frame(cbind(rownames(vi_dt$importance),round(vi_dt$importance$X1,4)))
names(vi_dt_df)<-c("VarName","VI_DT")

vi_rf_df<-data.frame(cbind(rownames(vi_rf$importance),round(vi_rf$importance,4)))
names(vi_rf_df)<-c("VarName","VI_RF")

vi<-merge(merge(vi_lr_df,vi_dt_df),vi_rf_df)
vi<-vi[order(-vi$VI_RF),]
# Combine Diagnostic Metrics from all models into a single data table
diag<-as.data.frame(rbind(c("Logistic Regression",diag_lr,round(roc_lr$auc,4),round(pr_lr$auc.integral,4)),
                          c("Decision Tree",diag_dt,round(roc_dt$auc,4),round(pr_dt$auc.integral,4)),
                          c("Random Forest",diag_rf,round(roc_rf$auc,4),round(pr_rf$auc.integral,4)),
                          c("Baseline - LACE",diag_lace,round(roc_lace$auc,4),round(pr_lace$auc.integral,4))))
names(diag)[c(1,4,8,9)]<-c("Model","Precision","AUROC","AUPRC Integral")

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
  addTable(rtffile,ak,col.width=c(1.4,0.8,0.8))
  addNewLine(rtffile,n=1)
  addParagraph(rtffile,"Variable Importance")
  addTable(rtffile,vi,col.width=c(1.8,0.8,0.8,0.8))
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
    addParagraph(rtffile,"CM (LACE)")
    addTable(rtffile,CM_LACE$table,col.width=c(1.0,0.8,0.8))
    addNewLine(rtffile,n=1)
  decreaseIndent(rtffile)
  addPageBreak(rtffile)
  addParagraph(rtffile,"Diagnostic Metrics")
  addTable(rtffile,diag,col.width=c(1.25,0.75,0.55,0.7,0.55,0.55,0.75,0.65,0.65))
  addNewLine(rtffile,n=1)
  addParagraph(rtffile,"AUROC and AUPRC")
    increaseIndent(rtffile)
    addParagraph(rtffile,"Logistic Regression")
    addPng.RTF(rtffile,file = "auroc_lr.png", width = 2.6, height = 2.4) 
    addPng.RTF(rtffile,file = "auprc_lr.png", width = 2.6, height = 2.4) 
    addParagraph(rtffile,"Decision Tree")
    addPng.RTF(rtffile,file = "auroc_dt.png", width = 2.6, height = 2.4) 
    addPng.RTF(rtffile,file = "auprc_dt.png", width = 2.6, height = 2.4) 
    addNewLine(rtffile,n=1)
    addParagraph(rtffile,"Random Forest")
    addPng.RTF(rtffile,file = "auroc_rf.png", width = 2.6, height = 2.4) 
    addPng.RTF(rtffile,file = "auprc_rf.png", width = 2.6, height = 2.4) 
    addParagraph(rtffile,"Baseline LACE")
    addPng.RTF(rtffile,file = "auroc_lace.png", width = 2.6, height = 2.4) 
    addPng.RTF(rtffile,file = "auprc_lace.png", width = 2.6, height = 2.4) 
    addNewLine(rtffile,n=1)
  decreaseIndent(rtffile)
done(rtffile)