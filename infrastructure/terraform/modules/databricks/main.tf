# infrastructure/terraform/modules/databricks/main.tf
# Reusable Azure Databricks module

variable "prefix"           { type = string }
variable "resource_group"   { type = string }
variable "location"         { type = string }
variable "sku"              { type = string; default = "premium" }
variable "vnet_id"          { type = string }
variable "public_subnet"    { type = string }
variable "private_subnet"   { type = string }
variable "nsg_id"           { type = string }
variable "key_vault_id"     { type = string }
variable "tenant_id"        { type = string }
variable "min_workers"      { type = number; default = 2 }
variable "max_workers"      { type = number; default = 8 }
variable "tags"             { type = map(string); default = {} }

resource "azurerm_databricks_workspace" "dbw" {
  name                = "dbw-${var.prefix}"
  resource_group_name = var.resource_group
  location            = var.location
  sku                 = var.sku

  custom_parameters {
    no_public_ip        = true
    virtual_network_id  = var.vnet_id
    public_subnet_name  = var.public_subnet
    private_subnet_name = var.private_subnet
    public_subnet_network_security_group_association_id  = var.nsg_id
    private_subnet_network_security_group_association_id = var.nsg_id
  }
  tags = var.tags
}

# Databricks → Key Vault access
resource "azurerm_key_vault_access_policy" "dbw_kv" {
  key_vault_id = var.key_vault_id
  tenant_id    = var.tenant_id
  object_id    = azurerm_databricks_workspace.dbw.storage_account_identity[0].principal_id
  secret_permissions = ["Get", "List"]
}

output "workspace_id"   { value = azurerm_databricks_workspace.dbw.workspace_id }
output "workspace_url"  { value = azurerm_databricks_workspace.dbw.workspace_url }
output "workspace_name" { value = azurerm_databricks_workspace.dbw.name }
output "resource_id"    { value = azurerm_databricks_workspace.dbw.id }
