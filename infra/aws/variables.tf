variable "region" {
  type    = string
  default = "eu-west-3"
}

variable "project" {
  type    = string
  default = "velib-pipeline"
}

variable "suffix" {
  type        = string
  description = "Suffixe pour rendre le nom du bucket unique"
  default     = "demo"
}

variable "subnet_ids" {
  type        = list(string)
  description = "Sous-réseaux privés pour MSK Serverless"
  default     = []
}

variable "security_group_ids" {
  type    = list(string)
  default = []
}
