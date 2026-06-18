# =============================================================================
# stg.tfvars — Staging Environment
# Healthcare Data Platform — Azure Infrastructure
# =============================================================================

# Environment
environment  = "stg"
location     = "eastus2"
project_name = "healthcare"

# Databricks
databricks_sku         = "premium"
databricks_min_workers = 2
databricks_max_workers = 8

# ADF
adf_git_branch      = "release/staging"
adf_git_repo_name   = "healthcare-data-platform"
adf_git_account     = "Prudhvi-Gaddam"
adf_git_project     = "healthcare-data-platform"
adf_git_root_folder = "/adf-pipelines"

# ADLS
adls_replication    = "ZRS"     # Zone redundant (staging)
adls_tier           = "Standard"

# Azure SQL
sql_sku             = "S2"
sql_max_size_gb     = 50

# Networking
vnet_address_space          = "10.20.0.0/16"
databricks_public_subnet    = "10.20.1.0/24"
databricks_private_subnet   = "10.20.2.0/24"
azure_sql_subnet            = "10.20.3.0/24"

# Self-Hosted IR
shir_name = "shir-onprem-stg"

# Tags
tags = {
  environment = "staging"
  project     = "healthcare-data-platform"
  owner       = "prudhvi.gaddam"
  team        = "data-engineering"
  cost_center = "DE-001"
  managed_by  = "terraform"
}
