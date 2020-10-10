data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

resource "aws_cloudwatch_event_rule" "lambda_trigger" {
  name_prefix = var.deployment_name
  description         = "Lambda trigger to extract data from sources"
  schedule_expression = "rate(5 minutes)"
}

resource "aws_cloudwatch_event_target" "lambda" {
  rule      = aws_cloudwatch_event_rule.lambda_trigger.name
  target_id = "Lambda"
  arn       = aws_lambda_function.lambda_etl.arn
}

resource "aws_lambda_permission" "allow_cloudwatch" {
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_etl.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.lambda_trigger.arn
}

resource "aws_lambda_function" "lambda_etl" {
  filename         = "etlfunction.zip"
  function_name    = "ETLChallenge-ExtractData"
  role             = aws_iam_role.lambda_role[0].arn
  handler          = "process_data.lambda_handler"
  source_code_hash = filebase64sha256("etlfunction.zip")
  runtime          = "python3.8"
  timeout          = 30
  layers           = ["arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:layer:${var.commonlib}"]
  environment {
    variables = {
      nyt            = var.datasource_nyt
      jhk            = var.datasource_jhk
      ddb_table_name = aws_dynamodb_table.etlchallenge_ddb.id
    }
  }
}

resource "aws_lambda_function" "lambda_ddb_streams" {
  filename         = "ddbstreamsfunction.zip"
  function_name    = "ETLChallenge-ProcessDDBStreams"
  role             = aws_iam_role.lambda_role[1].arn
  handler          = "process_streams.lambda_handler"
  source_code_hash = filebase64sha256("ddbstreamsfunction.zip")
  timeout          = 180
  runtime          = "python3.8"
  layers           = ["arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:layer:${var.commonlib}"]
  environment {
    variables = {
      gkey    = var.gkey
      gsheets = var.gsheets[terraform.workspace]
      from_email = var.from_email[terraform.workspace]
    }
  }
}

resource "aws_lambda_event_source_mapping" "ddbstreams_mapping" {
  event_source_arn                   = aws_dynamodb_table.etlchallenge_ddb.stream_arn
  function_name                      = aws_lambda_function.lambda_ddb_streams.arn
  batch_size                         = 50
  maximum_batching_window_in_seconds = 15
  starting_position                  = "LATEST"
  maximum_retry_attempts             = 2
  destination_config {
    on_failure {
      destination_arn = data.aws_sns_topic.notify.arn
    }
  }
}

resource "aws_lambda_function_event_invoke_config" "etlprocess_mapping" {
  function_name                = aws_lambda_function.lambda_etl.function_name
  maximum_event_age_in_seconds = 60
  maximum_retry_attempts       = 2
  destination_config {
    on_failure {
      destination = data.aws_sns_topic.notify.arn
    }
    on_success {
      destination = data.aws_sns_topic.notify.arn
    }
  }
}

resource "aws_dynamodb_table" "etlchallenge_ddb" {
  name             = "ETLChallenge-ddb"
  billing_mode     = "PAY_PER_REQUEST"
  stream_enabled   = true
  stream_view_type = "NEW_IMAGE"
  hash_key         = "reported_month"
  range_key        = "date"

  attribute {
    name = "reported_month"
    type = "S"
  }

  attribute {
    name = "date"
    type = "S"
  }
}

data "aws_sns_topic" "notify" {
  name = "ETLChallenge"
}

resource "aws_iam_role_policy" "lambda_etl_rolepolicy" {
  name_prefix = var.deployment_name
  role   = aws_iam_role.lambda_role[0].id
  policy = <<EOF
{
      "Version": "2012-10-17",
      "Statement": [
          {
              "Sid": "ETLRolePolicy0",
              "Effect": "Allow",
              "Action": [
                  "sns:Publish",
                  "logs:CreateLogStream",
                  "logs:PutLogEvents",
                  "logs:CreateLogGroup",
                  "dynamodb:BatchWriteItem",
                  "dynamodb:PutItem",
                  "dynamodb:Query"
              ],
              "Resource": [
                  "${data.aws_sns_topic.notify.arn}",
                  "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*",
                  "${aws_dynamodb_table.etlchallenge_ddb.arn}"
              ]
          }
      ]
}
EOF
}


resource "aws_iam_role_policy" "lambda_ddbstreams_rolepolicy" {
  name_prefix = var.deployment_name
  role   = aws_iam_role.lambda_role[1].id
  policy = <<EOF
{
      "Version": "2012-10-17",
      "Statement": [
          {
              "Sid": "DDBStreamsRolePolicy",
              "Effect": "Allow",
              "Action": [
                  "sns:Publish",
                  "ssm:GetParameter",
                  "logs:PutLogEvents",
                  "logs:CreateLogStream",
                  "logs:CreateLogGroup",
                  "ses:SendEmail",
                  "dynamodb:ListStreams",
                  "dynamodb:GetShardIterator",
                  "dynamodb:DescribeStream",
                  "dynamodb:GetRecords"
              ],
              "Resource": [
                  "${data.aws_sns_topic.notify.arn}",
                  "arn:aws:ssm:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:parameter/${var.gkey}",
                  "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*",
                  "arn:aws:ses:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:identity/*",
                  "${aws_dynamodb_table.etlchallenge_ddb.stream_arn}"
              ]
          }
      ]
}
EOF
}

resource "aws_iam_role" "lambda_role" {
  name = "${var.deployment_name}-${var.lambda_func[count.index]}"
  count = 2
  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}
