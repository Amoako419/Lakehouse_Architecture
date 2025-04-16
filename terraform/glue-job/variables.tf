variable "glue_job_name" {
  description = "Name of the Glue Job"
  type        = string
}

variable "glue_script_path" {
  description = "S3 path to the Glue script"
  type        = string
}

variable "glue_job_role_arn" {
  description = "IAM role ARN for Glue job"
  type        = string
}

variable "glue_temp_dir" {
  description = "Temporary directory path in S3 for Glue"
  type        = string
}

variable "glue_worker_type" {
  description = "Worker type for Glue job"
  type        = string
}

variable "glue_number_of_workers" {
  description = "Number of workers for Glue job"
  type        = number
}

variable "glue_version" {
  description = "Glue version"
  type        = string
}

variable "glue_job_parameters" {
  description = "Glue job parameters"
  type        = map(string)
}
