# =============================================================================
# prod.tfvars — Production Environment
# Healthcare Data Platform — Azure Infrastructure
# =============================================================================

# Environment
environment  = "prod"
location     = "eastus2"
project_name = "healthcare"

# Databricks — larger clusters for production workloads
databricks_sku         = "premium"
databricks_min_workers = 4
databricks_max_workers = 16

# ADF
adf_git_branch      = "main"
adf_git_repo_name   = "healthcare-data-platform"
adf_git_account     = "Prudhvi-Gaddam"
adf_git_project     = "healthcare-data-platform"
adf_git_root_folder = "/adf-pipelines"

# ADLS — geo-redundant storage for production
adls_replication    = "GRS"     # Geo-redundant (prod)
adls_tier           = "Standard"

# Azure SQL — higher tier for production audit workloads
sql_sku             = "S3"
sql_max_size_gb     = 250

# Networking
vnet_address_space          = "10.30.0.0/16"
databricks_public_subnet    = "10.30.1.0/24"
databricks_private_subnet   = "10.30.2.0/24"
azure_sql_subnet            = "10.30.3.0/24"

# Self-Hosted IR
shir_name = "shir-onprem-prod"

# Tags
tags = {
  environment  = "production"
  project      = "healthcare-data-platform"
  owner        = "prudhvi.gaddam"
  team         = "data-engineering"
  cost_center  = "DE-001"
  managed_by   = "terraform"
  criticality  = "high"
  data_class   = "phi-safe"
  hipaa        = "true"
}
