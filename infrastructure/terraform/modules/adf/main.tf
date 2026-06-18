# infrastructure/terraform/modules/adf/main.tf
# Reusable Azure Data Factory module

variable "prefix"           { type = string }
variable "resource_group"   { type = string }
variable "location"         { type = string }
variable "key_vault_id"     { type = string }
variable "adls_account_id"  { type = string }
variable "git_account"      { type = string }
variable "git_repo"         { type = string }
variable "git_project"      { type = string }
variable "git_branch"       { type = string; default = "main" }
variable "git_root_folder"  { type = string; default = "/adf-pipelines" }
variable "shir_name"        { type = string; default = "shir-onprem" }
variable "tenant_id"        { type = string }
variable "tags"             { type = map(string); default = {} }

resource "azurerm_data_factory" "adf" {
  name                = "adf-${var.prefix}"
  resource_group_name = var.resource_group
  location            = var.location
  identity            { type = "SystemAssigned" }

  vsts_configuration {
    account_name    = var.git_account
    branch_name     = var.git_branch
    project_name    = var.git_project
    repository_name = var.git_repo
    root_folder     = var.git_root_folder
    tenant_id       = var.tenant_id
  }
  tags = var.tags
}

# ADF → ADLS Storage Blob Data Contributor
resource "azurerm_role_assignment" "adf_adls" {
  scope                = var.adls_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.adf.identity[0].principal_id
}

# ADF → Key Vault Secrets (read only)
resource "azurerm_key_vault_access_policy" "adf_kv" {
  key_vault_id = var.key_vault_id
  tenant_id    = var.tenant_id
  object_id    = azurerm_data_factory.adf.identity[0].principal_id
  secret_permissions = ["Get", "List"]
}

# Self-Hosted Integration Runtime for on-prem SQL Server
resource "azurerm_data_factory_integration_runtime_self_hosted" "shir" {
  name            = var.shir_name
  data_factory_id = azurerm_data_factory.adf.id
  description     = "Self-Hosted IR for on-premises SQL Server sources"
}

output "adf_id"           { value = azurerm_data_factory.adf.id }
output "adf_name"         { value = azurerm_data_factory.adf.name }
output "adf_principal_id" { value = azurerm_data_factory.adf.identity[0].principal_id }
output "shir_name"        { value = azurerm_data_factory_integration_runtime_self_hosted.shir.name }
output "shir_auth_key"    { value = azurerm_data_factory_integration_runtime_self_hosted.shir.auth_key_1; sensitive = true }
