select_question <- function (question) {
  ## this function opens a full dataset, generated from json files by another script
  ## and reshapes all answers into one column 
  
  setwd("C:\\Users\\Inna\\Box Sync\\ik-Food security (WSC)\\Processed data files")
  steps <- read.csv ("steps.csv", stringsAsFactors = FALSE)
  
  q <- grep(question, steps$text)
  
  answers <- data.frame(run=integer(), contact=character(), left_on=character(), question=character(), stringsAsFactors=FALSE)

  for (i in 1:length(q)) {
    k <- q[[i]]
    answers <- rbind(answers, cbind(steps$run[k+1], steps$contact[k+1], steps$left_on[k+1], steps$text[k+1]))
    }
  
  colnames(answers) <- c("run", "contact", "left_on", question)
  write.csv(answers, "answers one column.csv")
  
  }