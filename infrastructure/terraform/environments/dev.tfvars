# =============================================================================
# dev.tfvars — Development Environment
# Healthcare Data Platform — Azure Infrastructure
# =============================================================================

# Environment
environment  = "dev"
location     = "eastus2"
project_name = "healthcare"

# Databricks
databricks_sku      = "premium"
databricks_min_workers = 1
databricks_max_workers = 4

# ADF
adf_git_branch      = "develop"
adf_git_repo_name   = "healthcare-data-platform"
adf_git_account     = "Prudhvi-Gaddam"
adf_git_project     = "healthcare-data-platform"
adf_git_root_folder = "/adf-pipelines"

# ADLS
adls_replication    = "LRS"     # Locally redundant (dev only)
adls_tier           = "Standard"

# Azure SQL (audit database)
sql_sku             = "S1"
sql_max_size_gb     = 10

# Networking
vnet_address_space          = "10.10.0.0/16"
databricks_public_subnet    = "10.10.1.0/24"
databricks_private_subnet   = "10.10.2.0/24"
azure_sql_subnet            = "10.10.3.0/24"

# Self-Hosted IR
shir_name = "shir-onprem-dev"

# Tags
tags = {
  environment = "dev"
  project     = "healthcare-data-platform"
  owner       = "prudhvi.gaddam"
  team        = "data-engineering"
  cost_center = "DE-001"
  managed_by  = "terraform"
}
