terraform {
  backend "s3" {
    encrypt        = true
    workspace_key_prefix = "ETLChallenge"
  }
}
