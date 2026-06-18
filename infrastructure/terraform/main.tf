# =============================================================================
# main.tf — Healthcare Data Platform Azure Infrastructure
# Provisions: ADLS Gen2, ADF, Databricks, Key Vault, Azure SQL, VNet
# =============================================================================

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.80"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.30"
    }
  }

  # Remote state in Azure Blob Storage
  backend "azurerm" {
    resource_group_name  = "rg-healthcare-tfstate"
    storage_account_name = "sahealthcaretfstate"
    container_name       = "tfstate"
    key                  = "healthcare-platform.tfstate"
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy    = false
      recover_soft_deleted_key_vaults = true
    }
    resource_group {
      prevent_deletion_if_contains_resources = true
    }
  }
}

# =============================================================================
# Variables
# =============================================================================
variable "environment"             { type = string }
variable "location"                { type = string;  default = "eastus2" }
variable "project_name"            { type = string;  default = "healthcare" }
variable "databricks_sku"          { type = string;  default = "premium" }
variable "databricks_min_workers"  { type = number;  default = 2 }
variable "databricks_max_workers"  { type = number;  default = 8 }
variable "adf_git_branch"          { type = string;  default = "main" }
variable "adf_git_repo_name"       { type = string }
variable "adf_git_account"         { type = string }
variable "adf_git_project"         { type = string }
variable "adf_git_root_folder"     { type = string;  default = "/adf-pipelines" }
variable "adls_replication"        { type = string;  default = "ZRS" }
variable "adls_tier"               { type = string;  default = "Standard" }
variable "sql_sku"                 { type = string;  default = "S2" }
variable "sql_max_size_gb"         { type = number;  default = 50 }
variable "vnet_address_space"      { type = string }
variable "databricks_public_subnet"  { type = string }
variable "databricks_private_subnet" { type = string }
variable "azure_sql_subnet"        { type = string }
variable "shir_name"               { type = string }
variable "tags"                    { type = map(string); default = {} }

locals {
  prefix = "${var.project_name}-${var.environment}"
  common_tags = merge(var.tags, {
    project    = var.project_name
    managed_by = "terraform"
  })
}

data "azurerm_client_config" "current" {}

# =============================================================================
# Resource Group
# =============================================================================
resource "azurerm_resource_group" "main" {
  name     = "rg-${local.prefix}"
  location = var.location
  tags     = local.common_tags
}

# =============================================================================
# Virtual Network (for Databricks private deployment)
# =============================================================================
resource "azurerm_virtual_network" "main" {
  name                = "vnet-${local.prefix}"
  resource_group_name = azurerm_resource_group.main.name
  location            = var.location
  address_space       = [var.vnet_address_space]
  tags                = local.common_tags
}

resource "azurerm_subnet" "databricks_public" {
  name                 = "snet-databricks-public"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = [var.databricks_public_subnet]

  delegation {
    name = "databricks-delegation"
    service_delegation {
      name = "Microsoft.Databricks/workspaces"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/join/action",
        "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
        "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action"
      ]
    }
  }
}

resource "azurerm_subnet" "databricks_private" {
  name                 = "snet-databricks-private"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = [var.databricks_private_subnet]

  delegation {
    name = "databricks-delegation"
    service_delegation {
      name = "Microsoft.Databricks/workspaces"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/join/action",
        "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
        "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action"
      ]
    }
  }
}

# =============================================================================
# ADLS Gen2 — Medallion Architecture
# =============================================================================
resource "azurerm_storage_account" "adls" {
  name                     = "adls${var.project_name}${var.environment}"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = var.location
  account_tier             = var.adls_tier
  account_replication_type = var.adls_replication
  account_kind             = "StorageV2"
  is_hns_enabled           = true        # Hierarchical Namespace = ADLS Gen2
  min_tls_version          = "TLS1_2"
  enable_https_traffic_only = true

  blob_properties {
    delete_retention_policy { days = 30 }
    versioning_enabled = true
    change_feed_enabled = true
  }

  tags = local.common_tags
}

# Medallion layer containers
resource "azurerm_storage_container" "bronze" {
  name                  = "bronze"
  storage_account_name  = azurerm_storage_account.adls.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "silver" {
  name                  = "silver"
  storage_account_name  = azurerm_storage_account.adls.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold" {
  name                  = "gold"
  storage_account_name  = azurerm_storage_account.adls.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "landing" {
  name                  = "landing"
  storage_account_name  = azurerm_storage_account.adls.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "audit" {
  name                  = "audit"
  storage_account_name  = azurerm_storage_account.adls.name
  container_access_type = "private"
}

# =============================================================================
# Azure Key Vault
# =============================================================================
resource "azurerm_key_vault" "main" {
  name                       = "kv-${local.prefix}"
  resource_group_name        = azurerm_resource_group.main.name
  location                   = var.location
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  purge_protection_enabled   = true
  soft_delete_retention_days = 90

  # Terraform deployer access
  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id
    secret_permissions      = ["Get", "List", "Set", "Delete", "Purge", "Recover"]
    certificate_permissions = ["Get", "List"]
    key_permissions         = ["Get", "List"]
  }

  tags = local.common_tags
}

# =============================================================================
# Azure Data Factory
# =============================================================================
resource "azurerm_data_factory" "main" {
  name                = "adf-${local.prefix}"
  resource_group_name = azurerm_resource_group.main.name
  location            = var.location

  identity { type = "SystemAssigned" }

  # Git integration for CI/CD
  vsts_configuration {
    account_name    = var.adf_git_account
    branch_name     = var.adf_git_branch
    project_name    = var.adf_git_project
    repository_name = var.adf_git_repo_name
    root_folder     = var.adf_git_root_folder
    tenant_id       = data.azurerm_client_config.current.tenant_id
  }

  tags = local.common_tags
}

# ADF → ADLS access
resource "azurerm_role_assignment" "adf_adls_contributor" {
  scope                = azurerm_storage_account.adls.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.main.identity[0].principal_id
}

# ADF → Key Vault access
resource "azurerm_key_vault_access_policy" "adf_kv" {
  key_vault_id = azurerm_key_vault.main.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = azurerm_data_factory.main.identity[0].principal_id
  secret_permissions = ["Get", "List"]
}

# Self-Hosted Integration Runtime for on-prem SQL Server
resource "azurerm_data_factory_integration_runtime_self_hosted" "shir" {
  name            = var.shir_name
  data_factory_id = azurerm_data_factory.main.id
  description     = "Self-Hosted IR for on-premises SQL Server (Claims, Eligibility, Provider, Pharmacy)"
}

# =============================================================================
# Azure Databricks Workspace
# =============================================================================
resource "azurerm_databricks_workspace" "main" {
  name                = "dbw-${local.prefix}"
  resource_group_name = azurerm_resource_group.main.name
  location            = var.location
  sku                 = var.databricks_sku

  custom_parameters {
    no_public_ip        = true
    virtual_network_id  = azurerm_virtual_network.main.id
    public_subnet_name  = azurerm_subnet.databricks_public.name
    private_subnet_name = azurerm_subnet.databricks_private.name
    public_subnet_network_security_group_association_id  = azurerm_network_security_group.databricks.id
    private_subnet_network_security_group_association_id = azurerm_network_security_group.databricks.id
  }

  tags = local.common_tags
}

# Databricks NSG
resource "azurerm_network_security_group" "databricks" {
  name                = "nsg-databricks-${local.prefix}"
  resource_group_name = azurerm_resource_group.main.name
  location            = var.location
  tags                = local.common_tags
}

# =============================================================================
# Azure SQL Database (Audit & Metadata)
# =============================================================================
resource "azurerm_mssql_server" "audit" {
  name                         = "sql-${local.prefix}-audit"
  resource_group_name          = azurerm_resource_group.main.name
  location                     = var.location
  version                      = "12.0"
  administrator_login          = "sqladmin"
  administrator_login_password = "@{azurerm_key_vault_secret.sql_admin_password.value}"

  identity { type = "SystemAssigned" }
  tags = local.common_tags
}

resource "azurerm_mssql_database" "audit" {
  name         = "healthcare-audit-${var.environment}"
  server_id    = azurerm_mssql_server.audit.id
  sku_name     = var.sql_sku
  max_size_gb  = var.sql_max_size_gb
  zone_redundant = var.environment == "prod" ? true : false
  tags         = local.common_tags
}

# =============================================================================
# Outputs
# =============================================================================
output "resource_group_name"       { value = azurerm_resource_group.main.name }
output "adls_account_name"         { value = azurerm_storage_account.adls.name }
output "adls_primary_endpoint"     { value = azurerm_storage_account.adls.primary_dfs_endpoint }
output "adf_name"                  { value = azurerm_data_factory.main.name }
output "adf_identity_principal_id" { value = azurerm_data_factory.main.identity[0].principal_id }
output "databricks_workspace_url"  { value = azurerm_databricks_workspace.main.workspace_url }
output "databricks_workspace_id"   { value = azurerm_databricks_workspace.main.workspace_id }
output "key_vault_uri"             { value = azurerm_key_vault.main.vault_uri }
output "key_vault_name"            { value = azurerm_key_vault.main.name }
output "shir_name"                 { value = azurerm_data_factory_integration_runtime_self_hosted.shir.name }
output "sql_server_fqdn"           { value = azurerm_mssql_server.audit.fully_qualified_domain_name }
output "vnet_id"                   { value = azurerm_virtual_network.main.id }
