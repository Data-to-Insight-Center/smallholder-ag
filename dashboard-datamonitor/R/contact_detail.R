library(jsonlite)

host <- "http://localhost:8080/textit-api/zambia/"
from_date <- "2016-08-01T12:00:00.000Z"
to_date <- "2016-08-15T11:59:59.592Z"

contactsize <- fromJSON(sprintf("%scontactstats?from=%s&to=%s",host,from_date,to_date), flatten=TRUE)

count <- length(contactsize$total)

week_array <- c()
total_array <- c()
update_array <- c()
detail_array <- c()
value_array < c()
detail_total_array <- c()
detail_update_array <- c()
for(i in 1:count) {
  week <- paste(contactsize$fromDate[i], "to" , contactsize$toDate[i])
  week_array <- c(week_array, week)
  total_array <- c(total_array, contactsize$total[i])
  update_array <- c(update_array, contactsize$updated[i])
  value_array <- c(total_array,update_array)
  detail_total_array <- c(detail_total_array, "total")
  detail_update_array <- c(detail_update_array, "updated")
  detail_array <- c(detail_total_array,detail_update_array)
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

contact_map <- data.frame(week_detail=week_array,no_of_contacts=value_array,type=detail_array)

contact_graph <- ggplot(contact_map,aes(week_detail,no_of_contacts)) + geom_bar(aes(fill = type), stat = "identity", position = "dodge") + ggtitle("Number of total & modified farmers over week")
contact_graph_ly <- ggplotly(contact_graph)%>%
  layout(xaxis = x, yaxis = y)
print(contact_graph_ly)