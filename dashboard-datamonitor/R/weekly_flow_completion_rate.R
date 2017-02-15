library(jsonlite)

#host <- "http://localhost:8080/textit-api/zambia/"
host <- "http://smallholderag-test.d2i.indiana.edu:8080/textit-api/zambia/"
from_date <- "2017-02-06T12:00:00.000Z"
to_date <- "2017-02-13T11:59:59.592Z"

time_period <- paste(strsplit(from_date, "T")[[1]][1], strsplit(to_date, "T")[[1]][1], sep=" to ")

flowcompletion <- fromJSON(sprintf("%sflowcompletion?from=%s&to=%s",host,from_date,to_date), flatten=TRUE)

count <- length(flowcompletion$uuid)
# Load package
library(plotly)
library(ggplot2)
f <- list(
  family = "Courier New, monospace",
  size = 14,
  color = "#7f7f7f",
  dash = "dashed",
  boxpoints = "outliers"
)
x <- list(
  title = "Time (in days)",
  titlefont = f
)
y <- list(
  title = "Response Rate (in %)",
  titlefont = f
)

my_map <- data.frame()
vector1 <- c()
vector2 <- c()
vector3 <- c()
vector4 <- c()
total <- c()
avg <- c()

print(count)
for(i in 1:count) {
  
  mat_count <- length(flowcompletion$matrix[[i]]$perc)
  total <- c(total, flowcompletion$total_runs[i])
  if (flowcompletion$total_runs[i] != 0){
    for(j in 1:14) {
      if(j %% 2 == 0){
        if (!is.na(flowcompletion$matrix[[i]]$perc[j])){
          vector1 <- c(vector1, flowcompletion$matrix[[i]]$abs[j])
          vector2 <- c(vector2, flowcompletion$responded_matrix[[i]]$abs[j])
          vector3 <- c(vector3, flowcompletion$non_responded_matrix[[i]]$abs[j])
          vector4 <- c(vector4, j/2)
        }else{
          vector1 <- c(vector1, tail(vector1,1))
          vector2 <- c(vector2, tail(vector2,1))
          vector3 <- c(vector3, tail(vector3,1))
          vector4 <- c(vector4, j/2)
        }
      }
    }
  }
}

fillNAgaps <- function(x, firstBack=FALSE) {
  ## NA's in a vector or factor are replaced with last non-NA values
  ## If firstBack is TRUE, it will fill in leading NA's with the first
  ## non-NA value. If FALSE, it will not change leading NA's.
  
  # If it's a factor, store the level labels and convert to integer
  lvls <- NULL
  if (is.factor(x)) {
    lvls <- levels(x)
    x    <- as.integer(x)
  }
  
  goodIdx <- !is.na(x)
  
  # These are the non-NA values from x only
  # Add a leading NA or take the first good value, depending on firstBack   
  if (firstBack)   goodVals <- c(x[goodIdx][1], x[goodIdx])
  else goodVals <- c(NA, x[goodIdx])
  
  # Fill the indices of the output vector with the indices pulled from
  # these offsets of goodVals. Add 1 to avoid indexing to zero.
  fillIdx <- cumsum(goodIdx)+1
  
  x <- goodVals[fillIdx]
  
  # If it was originally a factor, convert it back
  if (!is.null(lvls)) {
    x <- factor(x, levels=seq_along(lvls), labels=lvls)
  }
  
  x
}

vector1 <- fillNAgaps(vector1, firstBack=TRUE)
vector2 <- fillNAgaps(vector2, firstBack=TRUE)
vector3 <- fillNAgaps(vector3, firstBack=TRUE)

vector5 <- c()
vector6 <- c()
vector7 <- c()
vector8 <- c()
a <- 1:length(vector1)
b <- 1:length(vector2)
c <- 1:length(vector3)
for (k in 1:7){
d <- vector1[seq(k, length(a), 7)]
e <- vector2[seq(k, length(b), 7)]
f <- vector3[seq(k, length(c), 7)]

vector5 <- c(vector5, (sum(d)*100/sum(total)))
vector6 <- c(vector6, (sum(e)*100/sum(total)))
vector7 <- c(vector7, (sum(f)*100/sum(total)))

if (k==1){
  vector8 <- c(vector8, paste(k, "day", sep="st "))
}else if(k ==2){
  vector8 <- c(vector8, paste(k, "day", sep="nd "))
}else if(k ==3){
  vector8 <- c(vector8, paste(k, "day", sep="rd "))
}else{
  vector8 <- c(vector8, paste(k, "day", sep="th "))
}

}

my_map <- data.frame(x1=vector8,y1=vector7, responded_type="no response")
my_map1 <- data.frame(x1=vector8,y1=vector5, responded_type="complete response")
my_map2 <- data.frame(x1=vector8,y1=vector6, responded_type="atleast one response")

merge_res_data <- rbind(my_map, my_map1, my_map2)

response_graph <- ggplot(merge_res_data,aes(x1,y1)) + geom_bar(aes(fill = responded_type), 
                  position = "dodge", stat = "identity") + 
                  ggtitle(paste("Farmers Response Rate - over time",time_period, sep=" ::: "))
response_graph_ly <- ggplotly(response_graph)%>%
  layout(xaxis = x, yaxis = y)
print(response_graph_ly)

