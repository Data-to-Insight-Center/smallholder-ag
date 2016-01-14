
## Fetch data from TextIt API
library(httr)

## Nov 16 flow metadata:
## ## "uuid": "9b869566-1763-45a9-8a5f-df38dac7484f", 


## Nov 23 flow metadata: 
## ## "uuid": "de5ce905-ad5f-4df3-9657-0424b8e19389", 


## Nov 30 metadata: 
## ## "uuid": "5f1d1112-4a6f-48d0-ab01-f01a2d26143c", 


## Dec 7 flow metadata:
## ## "uuid": "d7a5843c-b5bf-4b96-9776-346384181d29", 

nov16 <- "9b869566-1763-45a9-8a5f-df38dac7484f"
nov23 <- "de5ce905-ad5f-4df3-9657-0424b8e19389"
nov30 <- "5f1d1112-4a6f-48d0-ab01-f01a2d26143c"
dec7 <- "d7a5843c-b5bf-4b96-9776-346384181d29"

url <- paste0("https://textit.in/api/v1/runs.json?flow_uuid=", nov16)

url <- "https://textit.in/api/v1/flows.json"  #get a list of the flows on your account
url <- "https://textit.in/api/v1/contacts.json"  #get contacts



txt_token <- "Token c9fcc64a4281903cfeb0efa649cae3cc14c459d7"
req <- GET(url, add_headers("Authorization" = txt_token))
json <- content(req, as = "text")
runs <- fromJSON(json)
write(json, "test.json")
