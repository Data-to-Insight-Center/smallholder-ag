library(jsonlite)

host <- "http://localhost:8080/textit-api/zambia/"
#host <- "http://smallholderag-test.d2i.indiana.edu:8080/textit-api/zambia/"
from_date <- "2016-08-15T11:00:00.000Z"
to_date <- "2016-08-22T11:00:00.000Z"

delayrunsize <- fromJSON(sprintf("%sdelayedruns?from=%s&to=%s",host,from_date,to_date), flatten=TRUE)

count <- length(delayrunsize$uuid)

week_array <- c()
total_array <- c()
date_array <- c()
late_count_array <- c()
name_array < c()
for(i in 1:count) {
  #week <- paste(delayrunsize$start_date[i], "to" , delayrunsize$end_date[i])
  week_array <- c(week_array, delayrunsize$week[i])
  total_array <- c(total_array, delayrunsize$total_runs[i])
  late_count_array <- c(late_count_array, delayrunsize$late_runs[i])
  name_array <- c(name_array,delayrunsize$name[i])
}

# Load package
library(plotly)
library(ggplot2)
f <- list(
  family = "Courier New, monospace",
  size = 14,
  color = "#7f7f7f"
)
x <- list(
  title = "Time (in weeks)",
  titlefont = f
)
y <- list(
  title = "Numeber of farmers",
  titlefont = f
)

delay_run_map <- data.frame(week_detail=week_array,no_of_late_runs=late_count_array,type=name_array,no_of_total_runs=total_array)

delay_graph <- ggplot(delay_run_map,aes(week_detail,no_of_late_runs)) + geom_bar(aes(fill = type,no_of_total_runs=no_of_total_runs), stat = "identity", position = "dodge") + ggtitle("Number of total & modified farmers over week") + scale_y_continuous(trans='log2')
delay_graph_ly <- ggplotly(delay_graph)%>%
  layout(xaxis = x, yaxis = y)
print(delay_graph_ly)