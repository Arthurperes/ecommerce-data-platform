data "aws_s3_bucket" "data_lake" {
  bucket = "ecommerce-data-platform-mack-lab"
}

data "aws_instance" "streamlit" {
  filter {
    name   = "tag:Name"
    values = ["ecommerce-streamlit-dashboard"]
  }
}

data "aws_security_group" "streamlit_sg" {
  id = "sg-08ac83847756fbea5"
}
