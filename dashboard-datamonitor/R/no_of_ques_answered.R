library(jsonlite)

host <- "http://localhost:8080/textit-api/zambia/"
from_date <- "2016-08-01T12:00:00.000Z"
to_date <- "2016-08-15T11:59:59.592Z"

flowresponse1 <- fromJSON(sprintf("%sflowresponse?from=%s&to=%s&qCount=1",host,from_date,to_date), flatten=TRUE)
flowresponse2 <- fromJSON(sprintf("%sflowresponse?from=%s&to=%s&qCount=2",host,from_date,to_date), flatten=TRUE)
flowresponse3 <- fromJSON(sprintf("%sflowresponse?from=%s&to=%s&qCount=3",host,from_date,to_date), flatten=TRUE)
flowresponse4 <- fromJSON(sprintf("%sflowresponse?from=%s&to=%s&qCount=4",host,from_date,to_date), flatten=TRUE)
flowresponse5 <- fromJSON(sprintf("%sflowresponse?from=%s&to=%s&qCount=5",host,from_date,to_date), flatten=TRUE)
flowresponse6 <- fromJSON(sprintf("%sflowresponse?from=%s&to=%s&qCount=6",host,from_date,to_date), flatten=TRUE)
flowresponse7 <- fromJSON(sprintf("%sflowresponse?from=%s&to=%s&qCount=7",host,from_date,to_date), flatten=TRUE)

count <- length(flowresponse1$uuid)
# Load package
library(plotly)
library(ggplot2)
f <- list(
  family = "Courier New, monospace",
  size = 14,
  color = "#7f7f7f"
)
x <- list(
  title = "Min Answered # Of Question",
  titlefont = f
)
y <- list(
  title = "Response Rate (# of farmers)",
  titlefont = f
)

res_my_map1 <- data.frame()
res_vector1 <- c()
res_vector2 <- c()
res_vector3 <- c()
res_vector4 <- c()
res_vector5 <- c()
for(i in 1:count) {
  res_flow_name1 <- flowresponse1$flow_name[i]
  res_vector1 <- c(res_vector1, flowresponse1$responded[i])
  res_vector2 <- c(res_vector2, "1")
  res_vector3 <- c(res_vector3, res_flow_name1)
  res_vector4 <- c(res_vector4, flowresponse1$total_runs[i])
  res_vector5 <- c(res_vector5, flowresponse1$responded[i]-flowresponse1$responded[i+1])
}

res_my_map2 <- data.frame()
res2_vector1 <- c()
res2_vector2 <- c()
res2_vector3 <- c()
res2_vector4 <- c()
for(i in 1:count) {
  res_flow_name2 <- flowresponse2$flow_name[i]
  res2_vector1 <- c(res2_vector1, flowresponse2$responded[i])
  res2_vector2 <- c(res2_vector2, "2")
  res2_vector3 <- c(res2_vector3, res_flow_name2)
  res2_vector4 <- c(res2_vector4, flowresponse2$total_runs[i])
}

res_my_map3 <- data.frame()
res3_vector1 <- c()
res3_vector2 <- c()
res3_vector3 <- c()
res3_vector4 <- c()
for(i in 1:count) {
  res_flow_name3 <- flowresponse3$flow_name[i]
  res3_vector1 <- c(res3_vector1, flowresponse3$responded[i])
  res3_vector2 <- c(res3_vector2, "3")
  res3_vector3 <- c(res3_vector3, res_flow_name3)
  res3_vector4 <- c(res3_vector4, flowresponse3$total_runs[i])
}

res_my_map4 <- data.frame()
res4_vector1 <- c()
res4_vector2 <- c()
res4_vector3 <- c()
res4_vector4 <- c()
for(i in 1:count) {
  res_flow_name4 <- flowresponse4$flow_name[i]
  res4_vector1 <- c(res4_vector1, flowresponse4$responded[i])
  res4_vector2 <- c(res4_vector2, "4")
  res4_vector3 <- c(res4_vector3, res_flow_name4)
  res4_vector4 <- c(res4_vector4, flowresponse4$total_runs[i])
}

res_my_map5 <- data.frame()
res5_vector1 <- c()
res5_vector2 <- c()
res5_vector3 <- c()
res5_vector4 <- c()
for(i in 1:count) {
  res_flow_name5 <- flowresponse5$flow_name[i]
  res5_vector1 <- c(res5_vector1, flowresponse5$responded[i])
  res5_vector2 <- c(res5_vector2, "5")
  res5_vector3 <- c(res5_vector3, res_flow_name5)
  res5_vector4 <- c(res5_vector4, flowresponse5$total_runs[i])
}

res_my_map6 <- data.frame()
res6_vector1 <- c()
res6_vector2 <- c()
res6_vector3 <- c()
res6_vector4 <- c()
for(i in 1:count) {
  res_flow_name6 <- flowresponse6$flow_name[i]
  res6_vector1 <- c(res6_vector1, flowresponse6$responded[i])
  res6_vector2 <- c(res6_vector2, "6")
  res6_vector3 <- c(res6_vector3, res_flow_name6)
  res6_vector4 <- c(res6_vector4, flowresponse6$total_runs[i])
}

res_my_map7 <- data.frame()
res7_vector1 <- c()
res7_vector2 <- c()
res7_vector3 <- c()
res7_vector4 <- c()
for(i in 1:count) {
  res_flow_name7 <- flowresponse7$flow_name[i]
  res7_vector1 <- c(res7_vector1, flowresponse7$responded[i])
  res7_vector2 <- c(res7_vector2, "7")
  res7_vector3 <- c(res7_vector3, res_flow_name7)
  res7_vector4 <- c(res7_vector4, flowresponse7$total_runs[i])
}

res_my_map1 <- data.frame(x1=res_vector2,responded=res_vector1, flow_name=res_vector3, total=res_vector4, exact_val=res_vector1-res2_vector1, responded_total=sum(res_vector1), exact_val_total=sum(res_vector1)-sum(res2_vector1))
res_my_map2 <- data.frame(x1=res2_vector2,responded=res2_vector1, flow_name=res2_vector3, total=res2_vector4, exact_val=res2_vector1-res3_vector1, responded_total=sum(res2_vector1), exact_val_total=sum(res2_vector1)-sum(res3_vector1))
res_my_map3 <- data.frame(x1=res3_vector2,responded=res3_vector1, flow_name=res3_vector3, total=res3_vector4, exact_val=res3_vector1-res4_vector1, responded_total=sum(res3_vector1), exact_val_total=sum(res3_vector1)-sum(res4_vector1))
res_my_map4 <- data.frame(x1=res4_vector2,responded=res4_vector1, flow_name=res4_vector3, total=res4_vector4, exact_val=res4_vector1-res5_vector1, responded_total=sum(res4_vector1), exact_val_total=sum(res4_vector1)-sum(res5_vector1))
res_my_map5 <- data.frame(x1=res5_vector2,responded=res5_vector1, flow_name=res5_vector3, total=res5_vector4, exact_val=res5_vector1-res6_vector1, responded_total=sum(res5_vector1), exact_val_total=sum(res5_vector1)-sum(res6_vector1))
res_my_map6 <- data.frame(x1=res6_vector2,responded=res6_vector1, flow_name=res6_vector3, total=res6_vector4, exact_val=res6_vector1-res7_vector1, responded_total=sum(res6_vector1), exact_val_total=sum(res6_vector1)-sum(res7_vector1))
res_my_map7 <- data.frame(x1=res7_vector2,responded=res7_vector1, flow_name=res7_vector3, total=res7_vector4, exact_val=res7_vector1, responded_total=sum(res7_vector1), exact_val_total=sum(res7_vector1))

res_vector5 <- c(res_vector5, res_my_map1$responded-res_my_map2$responded)

merge_res_data <- rbind(res_my_map1, res_my_map2, res_my_map3, res_my_map4, res_my_map5, res_my_map6, res_my_map7)

p1 <- ggplot(merge_res_data, aes(x1,responded,total_runs=total)) + geom_bar(aes(fill = flow_name), position = "dodge", stat="identity", size=1) + labs(title="Farmers responses for no of questions", x="min answered no of question",y="no of responses")

p4 <- ggplot(merge_res_data, aes(x1,exact_val,total_runs=total)) + geom_bar(aes(fill = flow_name), position = "dodge", stat="identity") + labs(title="Farmers responses for no of questions", x="exactly answered no of question",y="no of responses")

p2 <- ggplot(merge_res_data, aes(x1,responded_total,total_runs=total)) + geom_bar(aes(fill = x1), stat="identity") + labs(title="Farmers responses for no of questions", x="min answered no of question", y="no of responses")

p5 <- ggplot(merge_res_data, aes(x1,exact_val_total,total_runs=total)) + geom_bar(aes(fill = x1), stat="identity") + labs(title="Farmers responses for no of questions", x="exactly answered no of question", y="no of responses")

p3 <- ggplot(merge_res_data, aes(x1,responded,total_runs=total)) + geom_bar(aes(fill = flow_name), position = "stack", stat="identity") + labs(title="Farmers responses for no of questions", x="min answered no of question", y="no of responses")

p6 <- ggplot(merge_res_data, aes(x1,exact_val,total_runs=total)) + geom_bar(aes(fill = flow_name), position = "stack", stat="identity") + labs(title="Farmers responses for no of questions", x="exactly answered no of question", y="no of responses")

#source("http://peterhaschke.com/Code/multiplot.R")

multiplot <- function(..., plotlist=NULL, file, cols=1, layout=NULL) {
  require(grid)
  
  plots <- c(list(...), plotlist)
  
  numPlots = length(plots)
  
  if (is.null(layout)) {
    layout <- matrix(seq(1, cols * ceiling(numPlots/cols)),
                     ncol = cols, nrow = ceiling(numPlots/cols))
  }
  
  if (numPlots==1) {
    print(plots[[1]])
    
  } else {
    grid.newpage()
    pushViewport(viewport(layout = grid.layout(nrow(layout), ncol(layout))))
    
    for (i in 1:numPlots) {
      matchidx <- as.data.frame(which(layout == i, arr.ind = TRUE))
      
      print(plots[[i]], vp = viewport(layout.pos.row = matchidx$row,
                                      layout.pos.col = matchidx$col))
    }
  }
}

p <- multiplot(p1, p2, p3, p4, p5, p6, cols=2)
print(p)

gg1 <- ggplotly(p1)
gg2 <- ggplotly(p2)
gg3 <- ggplotly(p3)
gg4 <- ggplotly(p4)
gg5 <- ggplotly(p5)
gg6 <- ggplotly(p6)

p0 <- subplot(
  ggplotly(p1),
  ggplotly(p2),
  margin = 0.05
)%>%
  layout(xaxis = x, yaxis = y)

p00 <- subplot(
  ggplotly(p3),
  ggplotly(p4),
  ggplotly(p5),
  ggplotly(p6),
  margin = 0.05,
  nrows=2
)%>%
  layout(xaxis = x, yaxis = y)

print(gg1)
print(gg2)
print(gg3)
print(gg4)
print(gg5)
print(gg6)
