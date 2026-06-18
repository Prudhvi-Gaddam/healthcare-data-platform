# infrastructure/terraform/modules/adls/main.tf
# Reusable ADLS Gen2 module for Medallion Architecture

variable "prefix"           { type = string }
variable "resource_group"   { type = string }
variable "location"         { type = string }
variable "replication_type" { type = string; default = "ZRS" }
variable "account_tier"     { type = string; default = "Standard" }
variable "tags"             { type = map(string); default = {} }
variable "containers"       { type = list(string); default = ["bronze","silver","gold","landing","audit"] }

resource "azurerm_storage_account" "adls" {
  name                      = replace("adls${var.prefix}", "-", "")
  resource_group_name       = var.resource_group
  location                  = var.location
  account_tier              = var.account_tier
  account_replication_type  = var.replication_type
  account_kind              = "StorageV2"
  is_hns_enabled            = true
  min_tls_version           = "TLS1_2"
  enable_https_traffic_only = true

  blob_properties {
    delete_retention_policy { days = 30 }
    versioning_enabled  = true
    change_feed_enabled = true
  }
  tags = var.tags
}

resource "azurerm_storage_container" "containers" {
  for_each              = toset(var.containers)
  name                  = each.value
  storage_account_name  = azurerm_storage_account.adls.name
  container_access_type = "private"
}

output "storage_account_id"       { value = azurerm_storage_account.adls.id }
output "storage_account_name"     { value = azurerm_storage_account.adls.name }
output "primary_dfs_endpoint"     { value = azurerm_storage_account.adls.primary_dfs_endpoint }
output "primary_connection_string" { value = azurerm_storage_account.adls.primary_connection_string; sensitive = true }
