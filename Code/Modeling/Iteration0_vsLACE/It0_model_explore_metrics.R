set.seed(42);

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
              "randomForest",
              "e1071",
              "PerformanceAnalytics",
              "ROCR",
              "pROC",
              "PRROC",
              "R2HTML",
              "gbm",
              "neuropsychology",
              "MLmetrics")

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
library(MLmetrics)

  
  
#increate max print to 100
options(max.print=100)


  
#DATA IMPORT

# Import file locally via CSV
model <- read.csv(file="/home/cdsw/query-impala-25596.csv", header=T, sep=",")
  
# Create new Y as factor 
model$VTarget <- factor(model$is_followed_by_a_30d_readmit)

# Drop columns you don't intend to use
#drops <- c("cin_no", "is_followed_by_death_or_readmit", "is_followed_by_a_30d_readmit")
#model <- model[,setdiff(colnames(model),drops)]
  
#Rename if need be columns with unorthodox names

  
# SPLIT DATA SET & LEAVE ASIDE TEST SET
  
# Random sampling, partition data into training (70%), remaining for validation (30%)
inTrain <- createDataPartition(y=model$VTarget, p=0.7, list=F)
dtrain <- model[inTrain,]
dvalid <- model[-inTrain,]

  
# MODEL TRAINING

# Logistic Regression


  #Optimize accuracy
  
fitControl <- trainControl(method="cv", number=5, savePrediction=T)
logitReg_acc <- train(VTarget ~ los + acuity + cerebrovasculardisease +
                  peripheralvasculardisease + diabeteswithoutcomplications + congestiveheartfailure +
                  diabeteswithendorgandamage + chronicpulmonarydisease + mildliverorrenaldisease +
                  anytumor + dementia + connectivetissuedisease + aids + metastaticsolidtumor + er_visits,
                  data=dtrain, method="glm", 
                  metric = "Accuracy",
                  family=binomial(), trControl=fitControl)
pred_lr <- predict(logitReg_acc,dvalid)
ak_lr<-round(c(logitReg_acc$results[1,2],logitReg_acc$results[1,3]),4)
names(ak_lr)<-c("Accuracy","Kappa")
CM_LR <- confusionMatrix(pred_lr,dvalid$VTarget,positive='1') 
n_hit <- CM_LR$table[2,2]
n_miss <- CM_LR$table[1,2]
n_fa <- CM_LR$table[2,1]
n_cr <- CM_LR$table[1,1]
tmp <- dprime(n_hit, n_miss, n_fa, n_cr) #from neuropsychology package
dprime_lr <- tmp[1]
diag_lr<-round(c(CM_LR$overall[1:2],CM_LR$byClass[c(3,6,7,11)],dprime=as.numeric(dprime_lr)),4)
roc_lr <- roc.curve(scores.class0 = as.numeric(as.character(pred_lr[dvalid$VTarget=='1'])), 
                    scores.class1 = as.numeric(as.character(pred_lr[dvalid$VTarget=='0'])), curve=T)
pr_lr <- pr.curve(scores.class0 = as.numeric(as.character(pred_lr[dvalid$VTarget=='1'])), 
                  scores.class1 = as.numeric(as.character(pred_lr[dvalid$VTarget=='0'])), curve = T)
  
  #Optimize kappa
  
fitControl <- trainControl(method="cv", number=5, savePrediction=T)
logitReg_kappa <- train(VTarget ~ los + acuity + cerebrovasculardisease +
                  peripheralvasculardisease + diabeteswithoutcomplications + congestiveheartfailure +
                  diabeteswithendorgandamage + chronicpulmonarydisease + mildliverorrenaldisease +
                  anytumor + dementia + connectivetissuedisease + aids + metastaticsolidtumor + er_visits,
                  data=dtrain, method="glm", 
                  metric = "Kappa",
                  family=binomial(), trControl=fitControl)
pred_lr <- predict(logitReg_kappa,dvalid)
ak_lr<-round(c(logitReg_kappa$results[1,2],logitReg_kappa$results[1,3]),4)
names(ak_lr)<-c("Accuracy","Kappa")
CM_LR <- confusionMatrix(pred_lr,dvalid$VTarget,positive='1') 
n_hit <- CM_LR$table[2,2]
n_miss <- CM_LR$table[1,2]
n_fa <- CM_LR$table[2,1]
n_cr <- CM_LR$table[1,1]
tmp <- dprime(n_hit, n_miss, n_fa, n_cr) #from neuropsychology package
dprime_lr <- tmp[1]
diag_lr<-round(c(CM_LR$overall[1:2],CM_LR$byClass[c(3,6,7,11)],dprime=as.numeric(dprime_lr)),4)
roc_lr <- roc.curve(scores.class0 = as.numeric(as.character(pred_lr[dvalid$VTarget=='1'])), 
                    scores.class1 = as.numeric(as.character(pred_lr[dvalid$VTarget=='0'])), curve=T)
pr_lr <- pr.curve(scores.class0 = as.numeric(as.character(pred_lr[dvalid$VTarget=='1'])), 
                  scores.class1 = as.numeric(as.character(pred_lr[dvalid$VTarget=='0'])), curve = T)
  
  
  #Optimize F1
  
## See http://topepo.github.io/caret/training.html#metrics; via https://stackoverflow.com/questions/37666516/caret-package-custom-metric
F1 <- function(data, lev = NULL, model = NULL) {
  F1_val <- F1_Score(y_pred = data$pred, y_true = data$obs, positive = lev[1])
  c(F1 = F1_val)
}
fitControl  <- trainControl(method="cv", number=5, savePrediction=T, summaryFunction = F1)  
logitReg_F1 <- train(VTarget ~ los + acuity + cerebrovasculardisease +
                  peripheralvasculardisease + diabeteswithoutcomplications + congestiveheartfailure +
                  diabeteswithendorgandamage + chronicpulmonarydisease + mildliverorrenaldisease +
                  anytumor + dementia + connectivetissuedisease + aids + metastaticsolidtumor + er_visits,
                  data=dtrain, method="glm", 
                  metric = "F1",
                  trControl = fitControl)
pred_lr <- predict(logitReg_F1,dvalid)
ak_lr<-round(c(logitReg_F1$results[1,2],logitReg_F1$results[1,3]),4)
names(ak_lr)<-c("Accuracy","Kappa")
CM_LR <- confusionMatrix(pred_lr,dvalid$VTarget,positive='1') 
n_hit <- CM_LR$table[2,2]
n_miss <- CM_LR$table[1,2]
n_fa <- CM_LR$table[2,1]
n_cr <- CM_LR$table[1,1]
tmp <- dprime(n_hit, n_miss, n_fa, n_cr) #from neuropsychology package
dprime_lr <- tmp[1]
diag_lr<-round(c(CM_LR$overall[1:2],CM_LR$byClass[c(3,6,7,11)],dprime=as.numeric(dprime_lr)),4)
roc_lr <- roc.curve(scores.class0 = as.numeric(as.character(pred_lr[dvalid$VTarget=='1'])), 
                    scores.class1 = as.numeric(as.character(pred_lr[dvalid$VTarget=='0'])), curve=T)
pr_lr <- pr.curve(scores.class0 = as.numeric(as.character(pred_lr[dvalid$VTarget=='1'])), 
                  scores.class1 = as.numeric(as.character(pred_lr[dvalid$VTarget=='0'])), curve = T)

  
  #Optimize dprime
  #Optimize ROC
  #Optimize Recision-Recall AUC
    

# Decision Tree
fitControl <- trainControl(method="cv", number=5, savePrediction=T)
logitReg_acc <- train(VTarget ~ los + acuity + cerebrovasculardisease +
                  peripheralvasculardisease + diabeteswithoutcomplications + congestiveheartfailure +
                  diabeteswithendorgandamage + chronicpulmonarydisease + mildliverorrenaldisease +
                  anytumor + dementia + connectivetissuedisease + aids + metastaticsolidtumor + er_visits,
                  data=dtrain, method="ctree", 
                  metric = "accuracy", trControl=fitControl)
  
pred_lr <- predict(logitReg_acc,dvalid)
ak_lr<-round(c(logitReg_acc$results[1,2],logitReg_acc$results[1,3]),4)
names(ak_lr)<-c("Accuracy","Kappa")
CM_LR <- confusionMatrix(pred_lr,dvalid$VTarget,positive='1') 
n_hit <- CM_LR$table[2,2]
n_miss <- CM_LR$table[1,2]
n_fa <- CM_LR$table[2,1]
n_cr <- CM_LR$table[1,1]
tmp <- dprime(n_hit, n_miss, n_fa, n_cr) #from neuropsychology package
dprime_lr <- tmp[1]
diag_lr<-round(c(CM_LR$overall[1:2],CM_LR$byClass[c(3,6,7,11)],dprime=as.numeric(dprime_lr)),4)
roc_lr <- roc.curve(scores.class0 = as.numeric(as.character(pred_lr[dvalid$VTarget=='1'])), 
                    scores.class1 = as.numeric(as.character(pred_lr[dvalid$VTarget=='0'])), curve=T)
pr_lr <- pr.curve(scores.class0 = as.numeric(as.character(pred_lr[dvalid$VTarget=='1'])), 
                  scores.class1 = as.numeric(as.character(pred_lr[dvalid$VTarget=='0'])), curve = T)
  
  #Optimize kappa
  
fitControl <- trainControl(method="cv", number=5, savePrediction=T)
logitReg_kappa <- train(VTarget ~ los + acuity + cerebrovasculardisease +
                  peripheralvasculardisease + diabeteswithoutcomplications + congestiveheartfailure +
                  diabeteswithendorgandamage + chronicpulmonarydisease + mildliverorrenaldisease +
                  anytumor + dementia + connectivetissuedisease + aids + metastaticsolidtumor + er_visits,
                  data=dtrain, method="glm", 
                  metric = "Kappa",
                  family=binomial(), trControl=fitControl)
pred_lr <- predict(logitReg_kappa,dvalid)
ak_lr<-round(c(logitReg_kappa$results[1,2],logitReg_kappa$results[1,3]),4)
names(ak_lr)<-c("Accuracy","Kappa")
CM_LR <- confusionMatrix(pred_lr,dvalid$VTarget,positive='1') 
n_hit <- CM_LR$table[2,2]
n_miss <- CM_LR$table[1,2]
n_fa <- CM_LR$table[2,1]
n_cr <- CM_LR$table[1,1]
tmp <- dprime(n_hit, n_miss, n_fa, n_cr) #from neuropsychology package
dprime_lr <- tmp[1]
diag_lr<-round(c(CM_LR$overall[1:2],CM_LR$byClass[c(3,6,7,11)],dprime=as.numeric(dprime_lr)),4)
roc_lr <- roc.curve(scores.class0 = as.numeric(as.character(pred_lr[dvalid$VTarget=='1'])), 
                    scores.class1 = as.numeric(as.character(pred_lr[dvalid$VTarget=='0'])), curve=T)
pr_lr <- pr.curve(scores.class0 = as.numeric(as.character(pred_lr[dvalid$VTarget=='1'])), 
                  scores.class1 = as.numeric(as.character(pred_lr[dvalid$VTarget=='0'])), curve = T)
  
  
  #Optimize F1
  
## See http://topepo.github.io/caret/training.html#metrics; via https://stackoverflow.com/questions/37666516/caret-package-custom-metric
f1 <- function(data, lev = NULL, model = NULL) {
  f1_val <- F1_Score(y_pred = data$pred, y_true = data$obs, positive = lev[1])
  c(F1 = f1_val)
}

fitControl  <- trainControl(method="cv", number=5, savePrediction=T, summaryFunction = f1)  
logitReg_F1 <- train(VTarget ~ los + acuity + cerebrovasculardisease +
                  peripheralvasculardisease + diabeteswithoutcomplications + congestiveheartfailure +
                  diabeteswithendorgandamage + chronicpulmonarydisease + mildliverorrenaldisease +
                  anytumor + dementia + connectivetissuedisease + aids + metastaticsolidtumor + er_visits,
                  data=dtrain, method="glm", 
                  metric = "F1",
                  trControl = fitControl)
pred_lr <- predict(logitReg_F1,dvalid)
ak_lr<-round(c(logitReg_F1$results[1,2],logitReg_F1$results[1,3]),4)
names(ak_lr)<-c("Accuracy","Kappa")
CM_LR <- confusionMatrix(pred_lr,dvalid$VTarget,positive='1') 
n_hit <- CM_LR$table[2,2]
n_miss <- CM_LR$table[1,2]
n_fa <- CM_LR$table[2,1]
n_cr <- CM_LR$table[1,1]
tmp <- dprime(n_hit, n_miss, n_fa, n_cr) #from neuropsychology package
dprime_lr <- tmp[1]
diag_lr<-round(c(CM_LR$overall[1:2],CM_LR$byClass[c(3,6,7,11)],dprime=as.numeric(dprime_lr)),4)
roc_lr <- roc.curve(scores.class0 = as.numeric(as.character(pred_lr[dvalid$VTarget=='1'])), 
                    scores.class1 = as.numeric(as.character(pred_lr[dvalid$VTarget=='0'])), curve=T)
pr_lr <- pr.curve(scores.class0 = as.numeric(as.character(pred_lr[dvalid$VTarget=='1'])), 
                  scores.class1 = as.numeric(as.character(pred_lr[dvalid$VTarget=='0'])), curve = T)

  
  
# Random Forest
#levels(dtrain$followed_by_30d_readmit) <- make.names(levels(factor(train$is_followed_by_a_30d_readmit)))
rForest <- train( VTarget ~ los + acuity + cerebrovasculardisease +
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
                #CM_rf <- confusionMatrix(pred_rf,dvalid$VTarget,positive='1') 
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

gbmModel <- train(VTarget ~ los + acuity + cerebrovasculardisease +
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
                  #CM_gbm <- confusionMatrix(pred_gbm,dvalid$VTarget,positive='1') 
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
nnetModel <- train(VTarget ~ los + acuity + cerebrovasculardisease +
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
                  #CM_nnet <- confusionMatrix(pred_nnet,dvalid$VTarget,positive='1') 
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




