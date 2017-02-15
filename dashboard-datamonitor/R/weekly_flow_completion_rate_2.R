library(jsonlite)

#host <- "http://localhost:8080/textit-api/zambia/"
host <- "http://smallholderag-test.d2i.indiana.edu:8080/textit-api/zambia/"
from_date <- "2017-01-23T12:00:00.000Z"
to_date <- "2017-02-13T11:59:59.592Z"

time_period <- paste(strsplit(from_date, "T")[[1]][1], strsplit(to_date, "T")[[1]][1], sep=" to ")

calc_days <- difftime(to_date, from_date, units = "days")
calc_days_numeric <- as.numeric(calc_days, units="days")
print(calc_days_numeric)

#count <- length(flowcompletion$uuid)
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
  title = "Time (in week)",
  titlefont = f
)
y <- list(
  title = "Response Rate (in %)",
  titlefont = f
)

v1 <- c()
v2 <- c()
v3 <- c()
v5 <- c()
v6 <- c()
v7 <- c() 
v8 <- c()
v9 <- c() 
v10 <- c()
tot <- c()

v11 <- c()
v22 <- c()
v33 <- c()
tot1 <- c()

v111 <- c()
v222 <- c()
v333 <- c()
tot2 <- c()

responded_type <- c()

v_map <- data.frame()
v_map1 <- data.frame()

new_from_date <- paste(as.Date(from_date, format = "%Y-%m-%d"), "11:00:00.049Z", sep="T")
new_to_date <- paste(as.Date(to_date, format = "%Y-%m-%d"), "11:00:00.049Z", sep="T")
int_to_date <- paste(as.Date(new_from_date) + 7, "11:00:00.049Z", sep="T")
int_from_date <- paste(as.Date(new_to_date) - 7, "11:00:00.049Z", sep="T")

if (calc_days_numeric/7 == 1){
  flowcompletion <- fromJSON(sprintf("%sflowcompletion?from=%s&to=%s",host,new_from_date,int_to_date), flatten=TRUE)
  int <- c(strsplit(new_from_date, "T")[[1]][1],strsplit(int_to_date, "T")[[1]][1])
  week_count <- 1
}else if(calc_days_numeric/7 == 2){
  flowcompletion <- fromJSON(sprintf("%sflowcompletion?from=%s&to=%s",host,new_from_date,int_to_date), flatten=TRUE)
  int <- c(strsplit(new_from_date, "T")[[1]][1],strsplit(int_to_date, "T")[[1]][1])
  flowcompletion1 <- fromJSON(sprintf("%sflowcompletion?from=%s&to=%s",host,int_from_date,new_to_date), flatten=TRUE)
  int2 <- c(strsplit(int_from_date, "T")[[1]][1],strsplit(new_to_date, "T")[[1]][1])
  week_count <- 2
}else if(calc_days_numeric/7 == 3){
  flowcompletion <- fromJSON(sprintf("%sflowcompletion?from=%s&to=%s",host,new_from_date,int_to_date), flatten=TRUE)
  int <- c(strsplit(new_from_date, "T")[[1]][1],strsplit(int_to_date, "T")[[1]][1])
  flowcompletion1 <- fromJSON(sprintf("%sflowcompletion?from=%s&to=%s",host,int_to_date,int_from_date), flatten=TRUE)
  int2 <- c(strsplit(int_to_date, "T")[[1]][1],strsplit(int_from_date, "T")[[1]][1])
  flowcompletion2 <- fromJSON(sprintf("%sflowcompletion?from=%s&to=%s",host,int_from_date,new_to_date), flatten=TRUE)
  int3 <- c(strsplit(int_from_date, "T")[[1]][1],strsplit(new_to_date, "T")[[1]][1])
  week_count <- 3
}else if(calc_days_numeric/7 == 4){
  flowcompletion <- fromJSON(sprintf("%sflowcompletion?from=%s&to=%s",host,from_date,to_date), flatten=TRUE)
  int <- c(new_from_date, as.Date(new_from_date) + 7)
  int2 <- c(as.Date(new_from_date) + 7, as.Date(new_to_date) + 14)
  int3 <- c(as.Date(new_to_date) + 14, as.Date(new_to_date) - 7)
  int4 <- c(as.Date(new_to_date) - 7, new_to_date)
  week_count <- 4
}
print(flowcompletion1)

'%between%'<-function(x,rng) x>=rng[1] & x<=rng[2]

print(int)
print(int2)

for(i in 1:length(flowcompletion$uuid)) {
  mat_count <- c();
  if(flowcompletion$total_runs[i] != 0 && flowcompletion$deployed_on[i] %between% int){
    
    mat_count <- c(mat_count, length(flowcompletion$responded_matrix[[i]]$perc))
    mat_count <- c(mat_count, length(flowcompletion$matrix[[i]]$perc))
    mat_count <- c(mat_count, length(flowcompletion$non_responded_matrix[[i]]$perc))
    min_mat_count <- min(mat_count)
    tot <- c(tot, flowcompletion$total_runs[i])
    
    v3 <- c(v3, flowcompletion$non_responded_matrix[[i]]$abs[min_mat_count])
    v1 <- c(v1, flowcompletion$matrix[[i]]$abs[min_mat_count])
    v2 <- c(v2, flowcompletion$responded_matrix[[i]]$abs[min_mat_count])
    
  }
}

for(j in 1:length(flowcompletion1$uuid)) {
  mat_count1 <- c();
  if(flowcompletion1$total_runs[j] != 0 && flowcompletion1$deployed_on[j] %between% int2){
      
      mat_count1 <- c(mat_count1, length(flowcompletion1$responded_matrix[[j]]$perc))
      mat_count1 <- c(mat_count1, length(flowcompletion1$matrix[[j]]$perc))
      mat_count1 <- c(mat_count1, length(flowcompletion1$non_responded_matrix[[j]]$perc))
      min_mat_count1 <- min(mat_count1)
      tot1 <- c(tot1, flowcompletion1$total_runs[j])
      
      v33 <- c(v33, flowcompletion1$non_responded_matrix[[j]]$abs[min_mat_count1])
      v11 <- c(v11, flowcompletion1$matrix[[j]]$abs[min_mat_count1])
      v22 <- c(v22, flowcompletion1$responded_matrix[[j]]$abs[min_mat_count1])
      
      
    }
}

for(k in 1:length(flowcompletion2$uuid)) {
  mat_count2 <- c();
  if(flowcompletion2$total_runs[k] != 0 && flowcompletion2$deployed_on[k] %between% int3){
    
    mat_count2 <- c(mat_count2, length(flowcompletion2$responded_matrix[[k]]$perc))
    mat_count2 <- c(mat_count2, length(flowcompletion2$matrix[[k]]$perc))
    mat_count2 <- c(mat_count2, length(flowcompletion2$non_responded_matrix[[k]]$perc))
    min_mat_count2 <- min(mat_count2)
    tot2 <- c(tot2, flowcompletion2$total_runs[k])
    
    v333 <- c(v333, flowcompletion2$non_responded_matrix[[k]]$abs[min_mat_count2])
    v111 <- c(v111, flowcompletion2$matrix[[k]]$abs[min_mat_count2])
    v222 <- c(v222, flowcompletion2$responded_matrix[[k]]$abs[min_mat_count2])
    
    
  }
}
responded_type <- c("no response", "complete response", "atleast one response")

if (week_count == 1){
  v5 <- c(v5, (sum(v3)*100/sum(tot)))
  v5 <- c(v5, (sum(v1)*100/sum(tot)))
  v5 <- c(v5, (sum(v2)*100/sum(tot)))
  v8 <- c(v8, paste(int[1], int[2], sep=" to "))
}else if (week_count == 2){
  v6 <- c(v6, (sum(v33)*100/sum(tot1))) 
  v6 <- c(v6, (sum(v11)*100/sum(tot1)))
  v6 <- c(v6, (sum(v22)*100/sum(tot1)))
  v7 <- c(v7, paste(int2[1], int2[2], sep=" to "))
  
  v5 <- c(v5, (sum(v3)*100/sum(tot)))
  v5 <- c(v5, (sum(v1)*100/sum(tot)))
  v5 <- c(v5, (sum(v2)*100/sum(tot)))
  v8 <- c(v8, paste(int[1], int[2], sep=" to "))
}else if (week_count == 3){
  v9 <- c(v9, (sum(v333)*100/sum(tot2))) 
  v9 <- c(v9, (sum(v111)*100/sum(tot2)))
  v9 <- c(v9, (sum(v222)*100/sum(tot2)))
  v10 <- c(v10, paste(int3[1], int3[2], sep=" to "))
  
  v6 <- c(v6, (sum(v33)*100/sum(tot1))) 
  v6 <- c(v6, (sum(v11)*100/sum(tot1)))
  v6 <- c(v6, (sum(v22)*100/sum(tot1)))
  v7 <- c(v7, paste(int2[1], int2[2], sep=" to "))
  
  v5 <- c(v5, (sum(v3)*100/sum(tot)))
  v5 <- c(v5, (sum(v1)*100/sum(tot)))
  v5 <- c(v5, (sum(v2)*100/sum(tot)))
  v8 <- c(v8, paste(int[1], int[2], sep=" to "))
}

if (week_count == 1){
  v_map <- data.frame(x1=v8, y1=v5, responded_type=responded_type)
  response_graph <- ggplot(v_map, aes(x1,y1)) + geom_bar(aes(fill = responded_type), position = "dodge", stat = "identity") + 
    ggtitle("Farmers Response Rate - over time") + scale_y_continuous(limits = c(0,100)) + 
    scale_fill_manual("legend", values = c("atleast one response" = "#619CFF", "complete response" = "#00BA38", "no response" = "#F8766D"))
  
}else if (week_count == 2){
  v_map <- data.frame(x1=v8, y1=v5, responded_type=responded_type)
  v_map1 <- data.frame(x1=v7, y1=v6, responded_type=responded_type)
  merge_v_map <- rbind(v_map, v_map1)
  response_graph <- ggplot(merge_v_map, aes(x1,y1)) + geom_bar(aes(fill = responded_type), position = "dodge", stat = "identity") + 
    ggtitle("Farmers Response Rate - over time") + scale_y_continuous(limits = c(0,100)) + 
    scale_fill_manual("legend", values = c("atleast one response" = "#619CFF", "complete response" = "#00BA38", "no response" = "#F8766D"))

}else if (week_count == 3){
  v_map <- data.frame(x1=v8, y1=v5, responded_type=responded_type)
  v_map1 <- data.frame(x1=v7, y1=v6, responded_type=responded_type)
  v_map2 <- data.frame(x1=v10, y1=v9, responded_type=responded_type)
  merge_v_map <- rbind(v_map, v_map1, v_map2)
  response_graph <- ggplot(merge_v_map, aes(x1,y1)) + geom_bar(aes(fill = responded_type), position = "dodge", stat = "identity") + 
    ggtitle("Farmers Response Rate - over time") + scale_y_continuous(limits = c(0,100)) +
    scale_fill_manual("legend", values = c("atleast one response" = "#619CFF", "complete response" = "#00BA38", "no response" = "#F8766D"))
  
}

response_graph_ly <- ggplotly(response_graph)%>%
  layout(xaxis = x, yaxis = y)
print(response_graph_ly)