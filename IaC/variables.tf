variable "deployment_name" {
  default = "ETLChallenge"
}

variable "region" {
  type = string
  description = "Region to deploy"
}

variable "from_email" {
  type = string
  description = "Email Notification"
}

variable "datasource_nyt" {
  default = {
    staging = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv"
    production = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv"
  }
}

variable "datasource_jhk" {
  default = {
    staging = "https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv?opt_id=oeu1600640557504r0.1891205861858345"
    production = "https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv?opt_id=oeu1600640557504r0.1891205861858345"
  }
}

variable "commonlib" {
  type = string
  description = "Lambda Layer hosting Python Common libraries"
}

variable "gsheets" {
  type = string
  description = "Google Sheet to connect to Tableau"
}

variable "gkey" {
  type = string
  description = "Google Key to connect"
}

variable "lambda_func" {
  type = list
  descrption = "Lambda Functions"
  default = ["lambda_etl","lambda_ddbstreams"]
}

variable "workspace_iam_roles" {
  type = string
  description = "IAM Role to assume for deployment"
}
