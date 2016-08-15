library(jsonlite)

host <- "http://localhost:8080/dashboard-datamonitor/api/zambia/"
ques_type <- "rain"
from_date <- "2016-06-20T12:00:00.000Z"
to_date <- "2016-08-15T11:59:59.592Z"

questionanalysis <- fromJSON(sprintf("%squestionanalysis?type=%s&from=%s&to=%s",host,ques_type,from_date,to_date), flatten=TRUE)
ques_count <- length(questionanalysis$day)
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
  title = "Number of farmers",
  titlefont = f
)

ques_map <- data.frame()
week_array <- c()
ans_array <- c()
variable_array <- c()
for(i in 1:ques_count) {
  
  mat_count <- length(questionanalysis$matrix[[i]]$count)
  week <- questionanalysis$day[i]
  for(j in 1:mat_count) {
    ans_array = c(ans_array, questionanalysis$matrix[[i]]$count[j])
    if (questionanalysis$matrix[[i]]$anw[j] == "No"){
      variable_array<- c(variable_array, questionanalysis$matrix[[i]]$anw[j])
    }else if (questionanalysis$matrix[[i]]$anw[j] == "Yes"){
      variable_array<- c(variable_array, questionanalysis$matrix[[i]]$anw[j])
    }else if (questionanalysis$matrix[[i]]$anw[j] == "Other"){
      variable_array<- c(variable_array, questionanalysis$matrix[[i]]$anw[j])
    }
    week_array <- c(week_array, week)
  }
  
}

ques_map <- data.frame(week_detail=week_array,no_of_responses=ans_array, responses=variable_array)

rain_graph <- ggplot(ques_map,aes(week_detail,no_of_responses)) + geom_bar(aes(fill = responses), stat = "identity", position = "dodge") + ggtitle("Farmers responses to the 'Did it rain…' question")
rain_graph_ly <- ggplotly(rain_graph)%>%
  layout(xaxis = x, yaxis = y)
print(rain_graph_ly)