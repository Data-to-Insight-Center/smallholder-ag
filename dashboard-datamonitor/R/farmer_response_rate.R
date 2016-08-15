library(jsonlite)

host <- "http://localhost:8080/textit-api/zambia/"
from_date <- "2016-08-01T12:00:00.000Z"
to_date <- "2016-08-15T11:59:59.592Z"

flowcompletion <- fromJSON(sprintf("%sflowcompletion?from=%s&to=%s",host,from_date,to_date), flatten=TRUE)
flowresponse <- fromJSON(sprintf("%sflowresponse?from=%s&to=%s",host,from_date,to_date), flatten=TRUE)

count <- length(flowcompletion$uuid)
res_count <- length(flowresponse$uuid)
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
  title = "Time (in hours)",
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
vector5 <- c()
vector6 <- c()
for(i in 1:count) {
  
  mat_count <- length(flowcompletion$matrix[[i]]$perc)
  flow_name <- flowcompletion$flow_name[i]
  for(j in 1:mat_count) {
    variable <- paste(flow_name," - answered all question")
    vector1 <- c(vector1, flowcompletion$matrix[[i]]$perc[j])
    vector2 <- c(vector2, flowcompletion$matrix[[i]]$hour[j])
    vector3 <- c(vector3, flow_name)
    vector4 <- c(vector4, variable)
    vector5 <- c(vector5, flowcompletion$matrix[[i]]$abs[j])
    vector6 <- c(vector6, "answered all question")
  }
}

res_my_map <- data.frame()
res_vector1 <- c()
res_vector2 <- c()
res_vector3 <- c()
res_vector4 <- c()
res_vector5 <- c()
res_vector6 <- c()
for(i in 1:res_count) {
  
  res_mat_count <- length(flowresponse$matrix[[i]]$perc)
  res_flow_name <- flowresponse$flow_name[i]
  for(j in 1:res_mat_count) {
    variable <- paste(res_flow_name," - answered minimum one question")
    res_vector1 <- c(res_vector1, flowresponse$matrix[[i]]$perc[j])
    res_vector2 <- c(res_vector2, flowresponse$matrix[[i]]$hour[j])
    res_vector3 <- c(res_vector3, res_flow_name)
    res_vector4 <- c(res_vector4, variable)
    res_vector5 <- c(res_vector5, flowresponse$matrix[[i]]$abs[j])
    res_vector6 <- c(res_vector6, "answered minimum one question")
  }
}

my_map <- data.frame(x1=vector2,y1=vector1, flow_name= vector3, color_flow=vector6, abs_value=vector5, type=vector4)
res_my_map <- data.frame(x1=res_vector2,y1=res_vector1, flow_name= res_vector3, color_flow=res_vector6, abs_value=res_vector5, type=res_vector4)

number_ticks <- function(n) {function(limits) pretty(limits, n)}

merge_data <- rbind(my_map, res_my_map)

response_graph <- ggplot(merge_data,aes(x1,y1,color=flow_name, group=color_flow, value=abs_value, type=type)) + geom_line(stat = "identity") + geom_point() +scale_x_continuous(breaks=number_ticks(15)) +
  scale_y_continuous(breaks=number_ticks(10)) + ggtitle("Farmers Response Rate - over time")
response_graph_ly <- ggplotly(response_graph)%>%
  layout(xaxis = x, yaxis = y)
print(response_graph_ly)