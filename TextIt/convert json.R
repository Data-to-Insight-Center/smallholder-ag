json_convert <- function (fldr) {
## This function takes a string with folder name where file are (fldr) as an argument
## and converts multiple json files into one
## Example 1 fldr = "C:\\Users\\Inna\\Box Sync\\ik-Food security (WSC)\\Zambia 2014"
## Example 2 fldr = "C:\\Users\\Inna\\Box Sync\\ik-Food security (WSC)\\Zambia 2015\\Nov-Dec 2015"

## Currently the script doesn't work with files that are split into parts,
## i.e. when "count" doesn't match number of contacts in the file
## Need to add a check for that
  
  library(jsonlite)
  library(plyr)
  setwd(fldr)
  
  dir_list <- list.files(fldr)  # read the list of files from the folder into a variable
  
  steps <- data.frame(run=integer(), contact=character(), node = character(), arrived_on = character(), left_on = character(), text = character(), type = character(), value = character(), stringsAsFactors=FALSE)
  i = 1
  count = 0
  while (i <= length(dir_list))
  {
    filename <- dir_list[[i]]
    myjson <- fromJSON(filename)
    count <- count + myjson$count
    if (myjson$count > 0) {
      for(j in 1:myjson$count) {
        single_df <- cbind(myjson$results$run[[j]], myjson$results$contact[[j]], myjson$results$steps[[j]])
        colnames(single_df)[1] <- "run"
        colnames(single_df)[2] <- "contact"
        steps <- rbind(steps, single_df)
        }
    }
   i <- i + 1
  }
   write.csv(steps, "C:\\Users\\Inna\\Box Sync\\ik-Food security (WSC)\\Processed data files\\steps.csv")
   print (count)
   #return (steps)
}