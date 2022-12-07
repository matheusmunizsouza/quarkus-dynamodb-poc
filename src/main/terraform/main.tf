provider "aws" {
  region                      = "us-east-1"
  access_key                  = "localstack"
  secret_key                  = "localstack"
  skip_credentials_validation = true
  skip_requesting_account_id  = true
  skip_metadata_api_check     = true
  endpoints {
    dynamodb = "http://localhost:4566"
  }
}

resource "aws_dynamodb_table" "dynamodb-table" {
  name           = "person"
  billing_mode   = "PROVISIONED"
  read_capacity  = 1
  write_capacity = 1
  hash_key       = "firstName"
  range_key      = "lastName"

  attribute {
    name = "firstName"
    type = "S"
  }

  attribute {
    name = "lastName"
    type = "S"
  }

  attribute {
    name = "cpf"
    type = "S"
  }

  global_secondary_index {
    name            = "cpf_index"
    hash_key        = "cpf"
    write_capacity  = 1
    read_capacity   = 1
    projection_type = "ALL"
  }
}
